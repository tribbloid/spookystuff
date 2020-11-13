package org.apache.spark.rdd.spookystuf

import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.utils.SparkUISupport
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkEnv, TaskContext}
import org.scalatest.FunSpec

import scala.util.Random

object ExternalAppendOnlyArraySuite {}

class ExternalAppendOnlyArraySuite extends FunSpec with SparkUISupport {

  import com.tribbloids.spookystuff.testutils.TestHelper._

  describe("cachedOrComputeIterator") {

    val taskSize = 1000

    val numChildren = 10

    val parallelism: Int = {

      Random.nextInt(TestHelper.TestSC.defaultParallelism - 1) + 1

      //    TestHelper.TestSC.defaultParallelism

      //    1
    }
    val resultSize: Int = parallelism * numChildren * taskSize

    def externalArrayRDD: RDD[ExternalAppendOnlyArray[Int]] = {

      val rdd: RDD[ExternalAppendOnlyArray[Int]] = TestSC
        .parallelize(0 until parallelism, parallelism)
        .map { _ =>
          val taskContext = TaskContext.get()

          val v =
            new ExternalAppendOnlyArray[Int](
              s"Test-${taskContext.taskAttemptId()}",
              StorageLevel.MEMORY_AND_DISK_SER,
              () => SparkEnv.get.serializer
            )

          v
        }

      rdd.persist().count() // count() is important in cluster mode as it 'lock' each partition to its location

      Thread.sleep(1000)

      rdd
    }

    describe(parallelism.toString) {

      def delayToEliminateRacing(i: Int) = {

        /**
         * the purpose of this line is to avoid no children other than the first one to compute
         * others always read cached values without racing
         * 1: -> -> -> ->
         * 2: C  C  C
         * 3: C  C
         */
        // TODO: this setting still can't guarantee 100% of the time when parallelism is high, ignored at the moment

        Thread.sleep(i * 200)
      }

      it("can be shared by multiple tasks") {

        val rdd = externalArrayRDD

        val computed = new LongAccumulator()
        TestSC.register(computed)

        val result = (1 to numChildren).flatMap { i =>
          println(s"=== $i ===")

          val result: Array[Int] = rdd
            .mapPartitions { p =>
              delayToEliminateRacing(i)

              val externalArray = p.next()

              val computeItr = (1 to taskSize).iterator.map { v =>
                computed.add(1)
                v
              }

              val itr = externalArray.StartingFrom().CachedOrComputeIterator(() => computeItr)

              itr.slice(0, taskSize)
            }
            .collect()

          Thread.sleep(1000)

          assert(computed.value <= parallelism * taskSize * TestHelper.numWorkers)

          result
        }

        assert(result.size === resultSize)
        //      assert(result == Seq(1 to numChildren).flatMap(i => 1 to 1000))
      }

      it("... even if they are executed concurrently") {

        val rdd = externalArrayRDD

        val computed = new LongAccumulator()
        TestSC.register(computed)

        val children = (1 to numChildren).map { i =>
          rdd
            .mapPartitions { p =>
              delayToEliminateRacing(i)

              val externalArray = p.next()

              val computeItr = (1 to taskSize).iterator.map { v =>
                computed.add(1)
                v
              }

              val itr = externalArray.StartingFrom().CachedOrComputeIterator(() => computeItr)

              itr.slice(0, taskSize)
            }
        }

        val combined = new UnionRDD(TestSC, children)

        val result = combined.collect().toSeq

        assert(computed.value <= parallelism * taskSize * TestHelper.numWorkers) // 2 computers cannot share the same ExternalArray

        assert(result.size === resultSize)
        //      assert(result == Seq(1 to numChildren).flatMap(i => 1 to 1000))
      }

      it("race condition will not cause duplicated values") {

        val rdd = externalArrayRDD

        val computed = new LongAccumulator()
        TestSC.register(computed)

        val children = (1 to numChildren).map { _ =>
          rdd
            .mapPartitions { p =>
              val externalArray = p.next()

              val computeItr = (1 to taskSize).iterator.map { v =>
                computed.add(1)
                v
              }

              val itr = externalArray.StartingFrom().CachedOrComputeIterator(() => computeItr)

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
}
