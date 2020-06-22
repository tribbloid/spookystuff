package org.apache.spark.rdd.spookystuf

import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.testutils.TestHelper.TestSC
import com.tribbloids.spookystuff.utils.PreemptiveLocalOps
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuf.IncrementallyCachedRDDSuite.WithRDD
import org.apache.spark.sql.spookystuf.FastForwardingIterator
import org.apache.spark.util.LongAccumulator
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import scala.collection.mutable
import scala.collection.immutable
import scala.util.Random

class IncrementallyCachedRDDSuite extends FunSpec with BeforeAndAfterAll {

  val size = 100000
  val stride = 10000
  // scale makes all the difference

  val fromSeq: Seq[Int] = 0.until(size, stride)

  val commonRanges: Seq[(Int, Int)] = {

    fromSeq.map { from =>
      val to = from + stride - 1
      from -> to
    }
  }

  val overlap = 2000
  val overlappingRanges: Seq[(Int, Int)] = {

    fromSeq.map { from =>
      val to = from + stride + overlap - 1
      from -> to
    }
  }

  val skip = 500
  val skippingRanges: Seq[(Int, Int)] = {

    fromSeq.map { from =>
      val to = from + stride - skip - 1
      from -> to
    }
  }

  case class Child(
      numPartitions: Int
  ) extends FunSpec {
    override def suiteName: String = s"$numPartitions"

    val groundTruth: WithRDD = WithRDD(TestSC.parallelize(1 to size, numPartitions))

    it("block cache always compute the entire partition") {

      val u = WithRDD(TestSC.parallelize(1 to size, numPartitions))
      val sliced = u.cached.getSlice(0, 2)

      val n = sliced.size

      assert(n == numPartitions * 2)
      assert(u.globalCounter.value == size)
      assert(u.localCounter.get() <= size)
    }

    val facets: Seq[(WithRDD#Facet, String)] = {

      val collect = WithRDD(TestSC.parallelize(1 to size, numPartitions))

      val localItr = WithRDD(
        TestSC.parallelize(1 to size, numPartitions), { rdd =>
          PreemptiveLocalOps(8).ForRDD(rdd).toLocalIterator.toList
        }
      )

      Seq(
        collect.incrementallyCached -> "collect",
        collect.incrementallyCached_ff -> "collect w/ fast-forward",
        localItr.incrementallyCached -> "preemptive toLocalIterator",
        localItr.incrementallyCached_ff -> "preemptive toLocalIterator w/ fast-forward"
      )
    }

    facets.foreach {
      case (facet, name) =>
        describe(name) {

          it("should compute & cache necessary slice of the partition") {

            for ((from, to) <- commonRanges) {

              val sliced = facet.getSlice(from, to)

              assert(sliced === groundTruth.src.getSlice(from, to))

              assert(sliced.size === (to - from))
              assert(facet.outer.count === numPartitions * to)
            }
          }

          it("... if slices are overlapping") {

            for ((from, to) <- overlappingRanges) {

              val sliced = facet.getSlice(from, to)

              assert(sliced === groundTruth.src.getSlice(from, to))

              assert(sliced.size === (to - from))
              assert(facet.outer.count === numPartitions * to)
            }
          }

          it("... if slices have gaps") {

            for ((from, to) <- skippingRanges) {

              val sliced = facet.getSlice(from, to)

              assert(sliced === groundTruth.src.getSlice(from, to))

              assert(sliced.size === (to - from))
              assert(facet.outer.count === numPartitions * to)
            }
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

    def localCounter: AtomicInteger = localAccs.getOrElseUpdate(id, new AtomicInteger())

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

    case class FastFwd(
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

    lazy val incrementallyCached_ff: FastFwd = {
      FastFwd(
        new IncrementallyCachedRDD(rddWithCounter)
      )
    }
  }
}
