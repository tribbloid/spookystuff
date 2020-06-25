package org.apache.spark.rdd.spookystuf

import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.testutils.TestHelper.TestSC
import com.tribbloids.spookystuff.utils.{PreemptiveLocalOps, SparkUISupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.spookystuf.FastForwardingIterator
import org.apache.spark.util.LongAccumulator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import scala.collection.{immutable, mutable}
import scala.reflect.ClassTag
import scala.util.Random

object IncrementallyCachedRDDSuites {

  val localAccs = mutable.Map.empty[Long, AtomicInteger]

  case class WithRDD[T](
      self: RDD[T],
      renderFn: RDD[T] => List[Any] = { v: RDD[T] =>
        v.collect().toList
      }
  ) {

    implicit val elementClassTag: ClassTag[T] = self.elementClassTag

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

    val rddWithCounter: RDD[T] = self.map { v =>
      localCounter.getAndIncrement()
      globalCounter add 1
      //      println(v)
      v
    }

    trait Facet {

      def outer: WithRDD[T] = WithRDD.this

      def rdd: RDD[T]

      def getSlice(from: Int, to: Int): List[Any]

      def render(): List[Any] = renderFn(rdd)
    }

    case class Slow(
        rdd: RDD[T]
    ) extends Facet {

      def getSlice(from: Int, to: Int): List[Any] = renderFn(
        rdd
          .mapPartitions { itr =>
            itr.slice(from, to)
          }
      )
    }

    case class Fast(
        rdd: RDD[T]
    ) extends Facet {

      def getSlice(from: Int, to: Int): List[Any] = renderFn(
        rdd
          .mapPartitions {
            case itr: FastForwardingIterator[T] =>
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
        IncrementallyCachedRDD(rddWithCounter)
      )
    }

    lazy val incrementallyCached_ff: Fast = {
      Fast(
        IncrementallyCachedRDD(rddWithCounter)
      )
    }
  }

  case class Fixture(
      facet: () => WithRDD[_]#Facet,
      name: String,
      verifyData: Boolean = true
  )

  def fixture[T](
      facet: => WithRDD[T]#Facet,
      name: String,
      verifyData: Boolean = true
  ): Fixture = Fixture(() => facet, name, verifyData)

  import TestHelper.TestSQL.implicits._

  val datasetSize = 5000
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

    val groundTruth: WithRDD[Int] = WithRDD(src)

    it("block cache always compute the entire partition") {

      val u = WithRDD(src)
      val sliced = u.cached.getSlice(0, 2)

      val n = sliced.size

      assert(n == numPartitions * 2)
      assert(u.count == datasetSize)
    }

    val fixtures: Seq[Fixture] = {

      def useCollect: WithRDD[Int] = WithRDD(src)

      def useToLocalItr: WithRDD[Int] = WithRDD(
        src, { rdd =>
          PreemptiveLocalOps(8).ForRDD(rdd).toLocalIterator.toList
        }
      )

      def useCollect_InternalRow: WithRDD[InternalRow] = {

        val df = src.toDF().queryExecution.toRdd

        WithRDD(
          df, { rdd =>
            rdd
              .map(v => v.getInt(0))
              .collect()
              .toList
          }
        )
      }

      Seq(
        fixture(useCollect.incrementallyCached, "collect"),
        fixture(useCollect.incrementallyCached_ff, "collect w/ fast-forward"),
        fixture(useToLocalItr.incrementallyCached, "preemptive toLocalIterator"),
        fixture(useToLocalItr.incrementallyCached_ff, "preemptive toLocalIterator w/ fast-forward"),
        fixture(
          useCollect_InternalRow.incrementallyCached_ff,
          "collet InternalRows w/ fast-forward"
        )
      )
    }

    fixtures.foreach { fixture =>
      describe(fixture.name) {

        it("should behave identically to RDD persist on collect") {

          val facet = fixture.facet()

          for (_ <- fromSeq) {

            val list = facet.render()

            assert(list === groundTruth.src.render())

            assert(facet.outer.count === datasetSize)
          }
        }

        def specOnRanges(seq: Seq[(Int, Int)]): Unit = {

          val facet = fixture.facet()

          for ((from, to) <- seq) {

            val sliced = facet.getSlice(from, to)

            if (fixture.verifyData)
              assert(sliced === groundTruth.src.getSlice(from, to))

            assert(facet.outer.count === Math.min(numPartitions * to, datasetSize))
          }
        }

        it("should compute subset of the partition") {

          specOnRanges(commonRanges)
        }

        it("... if slices are overlapping") {

          specOnRanges(overlappingRanges)
        }

        it("... if slices have gaps") {

          specOnRanges(skippingRanges)
        }
      }
    }
  }
}

class IncrementallyCachedRDDSuites extends FunSpec with BeforeAndAfterAll {

  import IncrementallyCachedRDDSuites._

  override def nestedSuites: immutable.IndexedSeq[Child] = immutable.IndexedSeq(
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
