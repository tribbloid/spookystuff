package org.apache.spark.rdd.spookystuf

import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.testutils.TestHelper.TestSC
import com.tribbloids.spookystuff.utils.{PreemptiveLocalOps, SparkUISupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.spookystuf.FastForwardingIterator
import org.apache.spark.util.LongAccumulator
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import scala.collection.{immutable, mutable}
import scala.util.Random

class IncrementallyCachedRDDSuite extends FunSpec with BeforeAndAfterAll {

  import IncrementallyCachedRDDSuite._

  val datasetSize = 10000
  val stride = 1000

  val overlap: Int = stride * 2

  val skip: Int = stride / 2

  // scale makes all the difference

  val fromSeq: Seq[Int] = 0.to(datasetSize, stride)

  val commonRanges: Seq[(Int, Int)] = {

    fromSeq.map { from =>
      val to = from + stride - 1
      from -> to
    }
  }

  val overlappingRanges: Seq[(Int, Int)] = {

    fromSeq.map { from =>
      val to = from + stride + overlap - 1
      from -> to
    }
  }

  val skippingRanges: Seq[(Int, Int)] = {

    fromSeq.map { from =>
      val to = from + stride - skip - 1
      from -> to
    }
  }

  case class Child(
      numPartitions: Int
  ) extends FunSpec
      with SparkUISupport {
    override def suiteName: String = s"$numPartitions"

    val src: RDD[Int] = TestSC.parallelize(1 to datasetSize, numPartitions)

    val groundTruth: WithRDD = WithRDD(src)

    it("block cache always compute the entire partition") {

      val u = WithRDD(src)
      val sliced = u.cached.getSlice(0, 2)

      val n = sliced.size

      assert(n == numPartitions * 2)
      assert(u.globalCounter.value == datasetSize)
      assert(u.localCounter.get() <= datasetSize)
    }

    val fixtures: Seq[Fixture] = {

      def collect = WithRDD(src)

      def localItr = WithRDD(
        src, { rdd =>
          PreemptiveLocalOps(8).ForRDD(rdd).toLocalIterator.toList
        }
      )

      Seq(
        fixture(collect.incrementallyCached, "collect"),
        fixture(collect.incrementallyCached_ff, "collect w/ fast-forward"),
        fixture(localItr.incrementallyCached, "preemptive toLocalIterator"),
        fixture(localItr.incrementallyCached_ff, "preemptive toLocalIterator w/ fast-forward")
      )
    }

    fixtures.foreach { fixture =>
      describe(fixture.name) {

        def onRanges(seq: Seq[(Int, Int)]): Unit = {

          val facet = fixture.facet()

          for ((from, to) <- seq) {

            val sliced = facet.getSlice(from, to)

            assert(sliced === groundTruth.src.getSlice(from, to))
            assert(facet.outer.count === Math.min(numPartitions * to, datasetSize))
          }
        }

        it("should compute & cache necessary slice of the partition") {

          onRanges(commonRanges)
        }

        it("... if slices are overlapping") {

          onRanges(overlappingRanges)
        }

        it("... if slices have gaps") {

          onRanges(skippingRanges)
        }
      }
    }

  }

  override def nestedSuites: immutable.IndexedSeq[Suite] = immutable.IndexedSeq(
    Child(1),
    Child(3),
    Child(8),
    Child(21),
    Child(64)
  )

//  override def afterAll(): Unit = {
//
//    super.afterAll()
//
//    SCFunctions.blockUntilKill(999999)
//  }
}

object IncrementallyCachedRDDSuite {

  val localAccs = mutable.Map.empty[Long, AtomicInteger]

  case class WithRDD(
      self: RDD[Int],
      collector: RDD[Int] => List[Int] = _.collect().toList
  ) {

    val id: Long = Random.nextLong()

    val localCounter: AtomicInteger = localAccs.getOrElseUpdate(id, new AtomicInteger())

    val globalCounter = new LongAccumulator()
    TestSC.register(globalCounter, "count")

    def count: Long = {

      val local = localCounter.get()
      val global = globalCounter.value

      require(local <= global)

      Math.max(local, global)
    }

    val ffInvoked = new LongAccumulator()
    TestSC.register(ffInvoked, "ffInvoked")

    val rddWithCounter: RDD[Int] = self.map { v =>
      localCounter.getAndIncrement()
      globalCounter add 1
//      println(v)
      v
    }

    trait Facet {

      def outer: WithRDD = WithRDD.this

      def getSlice(from: Int, to: Int): List[Int]
    }

    case class Slow(
        rdd: RDD[Int]
    ) extends Facet {

      def getSlice(from: Int, to: Int): List[Int] = collector(
        rdd
          .mapPartitions { itr =>
            itr.slice(from, to)
          }
      )
    }

    case class Fast(
        rdd: RDD[Int]
    ) extends Facet {

      def getSlice(from: Int, to: Int): List[Int] = collector(
        rdd
          .mapPartitions {
            case itr: FastForwardingIterator[Int] =>
              ffInvoked.add(1)
              itr.fastForward(from).slice(0, to - from)
            case itr =>
              itr.slice(from, to)
          }
      )
    }

    lazy val cached: Slow = Slow(rddWithCounter.persist())

    lazy val src: Slow = Slow(self)

    lazy val incrementallyCached: Slow = {
      Slow(
        new IncrementallyCachedRDD(rddWithCounter)
      )
    }

    lazy val incrementallyCached_ff: Fast = {
      Fast(
        new IncrementallyCachedRDD(rddWithCounter)
      )
    }
  }

  case class Fixture(
      facet: () => WithRDD#Facet,
      name: String
  )

  def fixture(
      facet: => WithRDD#Facet,
      name: String
  ): Fixture = Fixture(() => facet, name)
}
