package org.apache.spark.sql.spookystuf

import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.spark.TaskContext
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.util.LongAccumulator
import org.scalatest.FunSpec

import scala.util.Random

object ExternalAppendOnlyArraySuite {

  val taskSize = 1000

  val parallelism: Int = {

    Random.nextInt(TestHelper.TestSC.defaultParallelism - 1) + 1

//    TestHelper.TestSC.defaultParallelism

//    1
  }

  val numChildren = 10

  val resultSize: Int = parallelism * numChildren * taskSize
}

class ExternalAppendOnlyArraySuite extends FunSpec {

  import ExternalAppendOnlyArraySuite._
  import com.tribbloids.spookystuff.testutils.TestHelper._

  describe("cachedOrComputeIterator") {

    def getRDD: RDD[ExternalAppendOnlyArray[Int]] = {

      println(s"=== parallelism = $parallelism ===")

      val rdd: RDD[ExternalAppendOnlyArray[Int]] = TestSC.parallelize(0 until parallelism, parallelism).map { _ =>
        val taskContext = TaskContext.get()

        val v = new ExternalAppendOnlyArray[Int](s"Test-${taskContext.taskAttemptId()}")

        v
      }

      rdd.persist() //.count()

      rdd
    }

    it("can be shared by multiple tasks") {

      val rdd = getRDD

      val computed = new LongAccumulator()
      TestSC.register(computed)

      val result = (1 to numChildren).flatMap { i =>
        println(s"=== $i ===")

        val result = rdd
          .mapPartitions { p =>
            val externalArray = p.next()

            val computeItr = (1 to taskSize).iterator.map { v =>
              computed.add(1)
              v
            }

            val itr = externalArray.StartingFrom().cachedOrComputeIterator(computeItr)

            itr.slice(0, taskSize)
          }
          .collect()

        assert(computed.value <= parallelism * taskSize)

        result
      }

      assert(result.size === resultSize)
//      assert(result == Seq(1 to numChildren).flatMap(i => 1 to 1000))
    }

    it("... even if they are executed concurrently") {

      val rdd = getRDD

      val computed = new LongAccumulator()
      TestSC.register(computed)

      val children = (1 to numChildren).map { i =>
        rdd
          .mapPartitions { p =>
            /**
              * the purpose of this line is to avoid no thread other than the first one to use its iterator and fast-forward
              * 1: -> -> -> ->
              * 2: C  C  C
              * 3: C  C
              */
            Thread.sleep(i * 1000)
            val externalArray = p.next()

            val computeItr = (1 to taskSize).iterator.map { v =>
              computed.add(1)
              v
            }

            val itr = externalArray.StartingFrom().cachedOrComputeIterator(computeItr)

            itr.slice(0, taskSize)
          }
      }

      val combined = new UnionRDD(TestSC, children)

      val result = combined.collect().toSeq

      assert(computed.value == parallelism * taskSize)

      assert(result.size === resultSize)
//      assert(result == Seq(1 to numChildren).flatMap(i => 1 to 1000))
    }

    it("race condition will not cause duplicated values") {

      val rdd = getRDD

      val computed = new LongAccumulator()
      TestSC.register(computed)

      val children = (1 to numChildren).map { i =>
        rdd
          .mapPartitions { p =>
            val externalArray = p.next()

            val computeItr = (1 to taskSize).iterator.map { v =>
              computed.add(1)
              v
            }

            val itr = externalArray.StartingFrom().cachedOrComputeIterator(computeItr)

            itr.slice(0, taskSize)
          }
      }

      val combined = new UnionRDD(TestSC, children)

      val result = combined.collect().toSeq

      assert(result.size === resultSize)
      //      assert(result == Seq(1 to numChildren).flatMap(i => 1 to 1000))
    }
  }
}
