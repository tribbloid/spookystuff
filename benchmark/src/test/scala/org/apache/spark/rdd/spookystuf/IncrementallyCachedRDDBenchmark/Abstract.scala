package org.apache.spark.rdd.spookystuf.IncrementallyCachedRDDBenchmark

import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.utils.Stopwatch
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuf.IncrementallyCachedRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.scalatest.{BeforeAndAfterEach, FunSpec}

import scala.util.Random

abstract class Abstract extends FunSpec with BeforeAndAfterEach {

  TestHelper.enableCheckpoint

  val datasetSize = 100000
  val elementSize = 100

  val numPartitions = 100

  val storageLevel: StorageLevel = StorageLevel.DISK_ONLY

  val stopwatch: Stopwatch = Stopwatch()

  val count: LongAccumulator = {

    val v = new LongAccumulator()
    TestHelper.TestSC.register(v)
    v
  }

  override def beforeEach(): Unit = {

    super.beforeEach()

    stopwatch.reset()
    count.setValue(0)
  }

  override def afterEach(): Unit = {

    stopwatch.reset()
    count.setValue(0)

    super.afterEach()
  }

  val src: RDD[String] = {

    val src = TestHelper.TestSC.parallelize(1 to datasetSize, numPartitions).map { i =>
      Random.nextString(10)
    }
    src.checkpoint()
    src.count()

    src
  }

  def getRDD: RDD[String] = {

    val count = this.count

    src.map { v =>
      count.add(1)
      v + v
    }
  }

  for (i <- 1 to 3) {
    describe(i.toString) {

      it("vanilla persist") {

        stopwatch {

          val _rdd = getRDD.persist(storageLevel)

          _rdd.collect()
          assert(count.value === datasetSize)
          stopwatch.split
          _rdd.collect()
          assert(count.value === datasetSize)

        }.logDuration { v =>
          println(s"vanilla persist : ${v.splitHistory}")
        }

      }

      // TODO: this is too slow at the moment
      it("incremental cache") {

        val _rdd = IncrementallyCachedRDD(getRDD, storageLevel)

        stopwatch {

          _rdd.collect()
          assert(count.value === datasetSize)
          stopwatch.split
          _rdd.collect()
          assert(count.value === datasetSize)
        }.logDuration { v =>
          println(s"incremental cache : ${v.splitHistory}")
        }

      }
    }
  }
}
