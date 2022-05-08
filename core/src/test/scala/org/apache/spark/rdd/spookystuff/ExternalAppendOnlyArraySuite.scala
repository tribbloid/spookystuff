package org.apache.spark.rdd.spookystuff

import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.utils.SparkUISupport
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}
import org.apache.spark.{SparkEnv, TaskContext}

object ExternalAppendOnlyArraySuite {}

case class ExternalAppendOnlyArraySuite(
    parallelism: Int,
    taskSize: Int = 100,
    numTestRuns: Int = 10,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
) extends FunSpecx
    with SparkUISupport {

  import com.tribbloids.spookystuff.testutils.TestHelper._

  import scala.collection.JavaConverters._

  val e_resultSize: Int = parallelism * numTestRuns * taskSize
  val e_computeTimes: Int = parallelism * taskSize // * TestHelper.numWorkers

  describe(s"cachedOrComputeIterator - $parallelism") {

    val storageLevel = this.storageLevel
    val taskSize = this.taskSize

    def createRefRDD: RDD[ExternalAppendOnlyArray.LocalRef[Int]] = {

      val rdd = TestSC
        .parallelize(0 until parallelism, parallelism)
        .mapPartitions { _ =>
          val taskContext = TaskContext.get()

          val v = new ExternalAppendOnlyArray.LocalRef[Int](
            s"Test-${taskContext.taskAttemptId()}",
            storageLevel,
            () => SparkEnv.get.serializer
          )

          Iterator(v)
        }

      rdd.persist().foreach(_ => {})
      // important in cluster mode as it 'lock' each partition to its location
      // TODO: In UnionRDD the scheduler may choose to recompute depending partition
      //  preemptively, and this assumption may be broken

      Thread.sleep(1000)

      rdd
    }

    // 2 computers cannot share the same ExternalArray

    it("can be shared by multiple tasks") {

      val rdd = createRefRDD

      val computed = new LongAccumulator()
      TestSC.register(computed, "computed")

      val computedVs = new CollectionAccumulator[Int]()
      TestSC.register(computedVs, "computedVs")

      val result = (1 to numTestRuns).flatMap { i =>
        println(s"=== $i ===")

        val result: Array[Int] = rdd
          .mapPartitions { p =>
//              delayToEliminateRacing(i)

            val ref = p.next()

            val computeItr = (1 to taskSize).iterator.map { v =>
              computed.add(1)
              computedVs.add(v)
              v
            }

            val itr = ref.get.StartingFrom().CachedOrComputeIterator { () =>
              computeItr
            }

            itr.slice(0, taskSize)
          }
          .collect()

        Thread.sleep(1000)

        assert(
          computed.value <= e_computeTimes,
          s"""
               |
               |(values)
               |
               |${computedVs.value.asScala.mkString("\t")}
               |""".stripMargin.trim
        )

        result
      }

      assert(result.size === e_resultSize)
      //      assert(result == Seq(1 to numChildren).flatMap(i => 1 to 1000))
    }

    def delayToEliminateRace(i: Int): Unit = {

      /**
        * the purpose of this line is to avoid no children other than the first one to compute
        * others always read cached values without racing
        * 1: C1 C2 C3 C4
        * 2: -> C1 C2 C3
        * 3: -> -> C1 C2
        */
      // TODO: this setting still can't guarantee 100% of the time when parallelism is high
      //  for Disk IO the write speed is much slower than read, thus allowing later tasks to catch up

      val ii = Math.min(i, 1)

      Thread.sleep(ii * 3000)
    }

    it("... even if they are executed concurrently") {

      val rdd = createRefRDD

      val computed = new LongAccumulator()
      TestSC.register(computed, "computed")

      val computedVs = new CollectionAccumulator[Int]()
      TestSC.register(computedVs, "computedVs")

      val children = (0 until numTestRuns).map { i =>
        println(s"=== $i ===")
        rdd
          .mapPartitions { p =>
            delayToEliminateRace(i)

            val ref = p.next()

            val computeItr = (1 to taskSize).iterator.map { v =>
              computed.add(1)
              computedVs.add(v)
              v
            }

            val itr = ref.get.StartingFrom().CachedOrComputeIterator { () =>
              computeItr
            }

            itr.slice(0, taskSize)
          }
      }

      val combined = new UnionRDD(TestSC, children)

      val result = combined.collect().toSeq

      assert(
        computed.value <= e_computeTimes * 2, // * 2 is used to tolerate preemptive redundant computations
        s"""
             |
             |(values)
             |
             |${computedVs.value.asScala.mkString("\t")}
             |""".stripMargin.trim
      )

      assert(result.size === e_resultSize)
      //      assert(result == Seq(1 to numChildren).flatMap(i => 1 to 1000))
    }

    it("race condition will not cause duplicated values") {

      val rdd = createRefRDD

      val computed = new LongAccumulator()
      TestSC.register(computed)

      val children = (1 to numTestRuns).map { _ =>
        rdd
          .mapPartitions { p =>
            val ref = p.next()

            val computeItr = (1 to taskSize).iterator.map { v =>
              computed.add(1)
              v
            }

            val itr = ref.get.StartingFrom().CachedOrComputeIterator(() => computeItr)

            itr.slice(0, taskSize)
          }
      }

      val combined = new UnionRDD(TestSC, children)

      val result = combined.collect().toSeq

      assert(result.size === e_resultSize)
      //      assert(result == Seq(1 to numChildren).flatMap(i => 1 to 1000))
    }
  }

}
