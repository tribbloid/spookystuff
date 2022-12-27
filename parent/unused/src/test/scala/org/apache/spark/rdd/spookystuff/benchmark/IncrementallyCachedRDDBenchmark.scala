package org.apache.spark.rdd.spookystuff.benchmark

import com.tribbloids.spookystuff.testutils.{FunSpecx, SubSuite, TestHelper}
import com.tribbloids.spookystuff.utils.Stopwatch
import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.spookystuff.IncrementallyCachedRDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.scalatest.BeforeAndAfterEach

import scala.util.Random

abstract class IncrementallyCachedRDDBenchmark extends FunSpecx with BeforeAndAfterEach with SubSuite {

  import TestHelper.TestSQL.implicits._

  TestHelper.enableCheckpoint

  val datasetSize: Int = 100000
  val elementSize: Int = 100

  val numPartitions: Int = 100

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

  val strSrc: RDD[String] = {

    val elementSize = this.elementSize

    val src = TestHelper.TestSC.parallelize(1 to datasetSize, numPartitions).map { _ =>
      Random.nextString(elementSize)
    }
    src.checkpoint()
    src.count()

    src
  }

  val rowSrc: RDD[InternalRow] = {

    val elementSize = this.elementSize

    val srcDF = TestHelper.TestSC
      .parallelize(1 to datasetSize, numPartitions)
      .map { _ =>
        (Random.nextInt(), Random.nextLong(), Random.nextDouble(), Random.nextString(elementSize))
      }
      .toDF()

    val src = srcDF.queryExecution.toRdd
    src.checkpoint()
    src.count()

    src
  }

  def getRDD: RDD[_] = {

    val count = this.count

    strSrc.map { v =>
      count.add(1)
      v
    }
  }

  lazy val serializerFactory: () => Serializer = () => SparkEnv.get.serializer

  for (i <- 1 to 3) {
    describe(i.toString) {

      it("persist") {

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

        val _rdd = IncrementallyCachedRDD(getRDD, storageLevel, serializerFactory)

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
