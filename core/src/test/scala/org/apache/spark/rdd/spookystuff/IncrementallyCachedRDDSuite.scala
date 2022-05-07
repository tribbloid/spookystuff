package org.apache.spark.rdd.spookystuff

import java.util.concurrent.atomic.AtomicLong

import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.testutils.TestHelper.TestSC
import com.tribbloids.spookystuff.utils.{SparkUISupport, Stopwatch}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.util.LongAccumulator
import org.scalatest.FunSpec

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

abstract class IncrementallyCachedRDDSuite[T](
    numPartitions: Int
) extends FunSpec
    with SparkUISupport
    with Product {

  TestHelper.enableCheckpoint

  import org.apache.spark.rdd.spookystuff.IncrementallyCachedRDDSuite._

  override def suiteName: String = productPrefix + " - " + numPartitions

  def getFacet: TestSubject[T]#Facet

  def subject: TestSubject[T] = getFacet.subject

  def verifyData: Boolean = true

  it("block cache always compute the entire partition") {

    val u = subject
    val sliced = u.persisted.getSlice(0, 2)

    val n = sliced.size

    assert(n == numPartitions * 2)
    assert(u.count == datasetSize)
  }

  val stopwatch = Stopwatch()

  describe("should behave identically to RDD.persist") {

    def fullRangeSpec[R](fn: TestSubject[T]#Facet => R): Unit = {

      val facet = getFacet

      for (i <- 0 until 3) {

        stopwatch.reset()

        val result = fn(facet)
        println(s"[$i] rendering takes ${stopwatch.split}ms")

        val gtResult = fn(subject.groundTruth)

        assert(result === gtResult)
        assert(facet.subject.count === datasetSize)

      }

      {

        def doClear(): Unit = {
          facet.target match {
            case rdd: IncrementallyCachedRDD[_] => rdd.clearIncrementalCache()
            case _                              =>
          }
        }

        doClear()

        val list = facet.render()

        assert(list === subject.groundTruth.render())
        assert(facet.subject.count === datasetSize * 2)

        doClear()
      }
    }

    it("on collecting") {

      fullRangeSpec { v =>
        v.render()
      }
    }

    it("on counting") {

      fullRangeSpec { v =>
        v.target.count()
      }
    }

    // TODO: redundant for different rendering
    // TODO: this doesn't work at the moment
    ignore("on checkpointing") {

      fullRangeSpec { v =>
        v.target.checkpoint()
        v.target.count()
      }
    }

    it("on collecting downstream") {

      fullRangeSpec { v =>
        val downstream = v.target.map { vv =>
          "" + vv
        }

        downstream.collect()
      }
    }

    it("on counting downstream") {

      fullRangeSpec { v =>
        val downstream = v.target.map { vv =>
          "" + vv
        }

        downstream.count()
      }
    }

    it("on checkpointing downstream") {

      fullRangeSpec { v =>
        val downstream = v.target.map { vv =>
          "" + vv

        }

        downstream.checkpoint()
        downstream.count()
      }
    }
  }

  def sliceSpec(seq: Seq[(Int, Int)]): Unit = {

    val facet = getFacet

    for ((from, to) <- seq) {

      stopwatch.reset()

      val sliced = facet.getSlice(from, to)

      println(s"$from to $to takes ${stopwatch.split}ms")

      val slicedGD = subject.groundTruth.getSlice(from, to)

      assert(sliced.size === slicedGD.size)

      if (verifyData)
        assert(sliced === slicedGD)

      assert(facet.subject.count === Math.min(numPartitions * to, datasetSize))
    }
  }

  it("should compute subset of the partition") {

    sliceSpec(commonRanges)
  }

  it("... if slices are overlapping") {

    sliceSpec(overlappingRanges)
  }

  it("... if slices have gaps") {

    sliceSpec(skippingRanges)
  }

}

object IncrementallyCachedRDDSuite {

  //  val datasetSize = 1000
  //  val stride = 100

  val datasetSize = 10000
  val stride = 1000

  val overlap: Int = stride * 2

  val skip: Int = stride / 2

  // scale makes all the difference

  val fromSeq: Seq[Int] = 0.to(datasetSize / 4, stride)

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

  val localCounters = mutable.Map.empty[Long, AtomicLong]

  case class TestSubject[T](
      source: RDD[T],
      rendering: RDD[T] => List[Any] = { v: RDD[T] =>
        v.collect().toList
      }
  ) {

    implicit val elementClassTag: ClassTag[T] = source.elementClassTag

    val id: Long = Random.nextLong()

    val localCounter: AtomicLong = localCounters.getOrElseUpdate(id, new AtomicLong())
    val globalCounter = new LongAccumulator()
    TestSC.register(globalCounter, "count")

    def count: Long = {

      val local = localCounter.get()
      val global = globalCounter.value

      require(local <= global)

      Math.max(local, global)
    }

    val ffInvoked = new LongAccumulator()
    TestSC.register(ffInvoked, "Fast forward invoked")

    trait Facet {

      final def subject: TestSubject[T] = TestSubject.this
      final def render(): List[Any] = rendering(target)

      def target: RDD[T]

      def getSlice(from: Int, to: Int): List[Any]

    }

    case class Slow(
        target: RDD[T]
    ) extends Facet {

      def getSlice(from: Int, to: Int): List[Any] = rendering(
        target
          .mapPartitions { itr =>
            itr.slice(from, to)
          }
      )
    }

    case class Fast(
        target: RDD[T]
    ) extends Facet {

      def getSlice(from: Int, to: Int): List[Any] = rendering(
        target
          .mapPartitions {
            case itr: FastForwardingIterator[T] =>
              ffInvoked.add(1)
              itr.drop(from).slice(0, to - from)
            case itr =>
              itr.slice(from, to)
          }
      )
    }

    lazy val rddWithCounter: RDD[T] = source.map { v =>
      localCounter.getAndIncrement()
      globalCounter add 1
      //      println(v)
      v
    }

    lazy val groundTruth: Slow = Slow(source)

    lazy val persisted: Slow = Slow(rddWithCounter.persist())

    lazy val incCached_slow: Slow = {
      Slow(
        IncrementallyCachedRDD(rddWithCounter)
      )
    }

    lazy val incCached: Fast = {
      Fast(
        IncrementallyCachedRDD(rddWithCounter)
      )
    }

    lazy val incCached_unsafe: Fast = {
      Fast(
        IncrementallyCachedRDD(
          rddWithCounter,
          serializerFactory = { () =>
            new UnsafeRowSerializer(1)
          }
        )
      )
    }
  }
}
