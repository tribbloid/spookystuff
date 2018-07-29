package com.tribbloids.spookystuff.utils

import java.io.File

import com.tribbloids.spookystuff.testutils.{FunSpecx, TestHelper}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.TimeoutException
import scala.util.Random

/**
  * Created by peng on 11/1/14.
  */
class SpookyUtilsSuite extends FunSpecx {

  import SpookyViews._

  import scala.concurrent.duration._

  it("canonizeUrn should clean ?:$&#"){
    val url = SpookyUtils.canonizeUrn("http://abc.com?re#k2$si")
    assert(url === "http/abc.com/re/k2/si")
  }

  it("asArray[Int]") {
    assert(SpookyUtils.asArray[Int](2).toSeq == Seq(2))
    assert(SpookyUtils.asArray[Int](Seq(1,2,3).iterator).toSeq == Seq(1,2,3))
    assert(SpookyUtils.asArray[Int](Seq(1, 2.2, "b")).toSeq == Seq(1))
  }


  it("asIterable[Int]") {
    assert(SpookyUtils.asIterable[Int](2) == Iterable(2))
    assert(SpookyUtils.asIterable[Int](Seq(1,2,3).iterator).toSeq == Iterable(1,2,3))
    assert(SpookyUtils.asIterable[Int](Seq(1, 2.2, "b")).toSeq == Iterable(1))
  }

  it("copyResourceToDirectory can extract a dependency's package in a jar") {
    val src = SpookyUtils.getCPResource("org/apache/log4j/xml").get
    val dst = CommonUtils.\\\(CommonConst.TEMP_DIR, "log4j")
    SpookyUtils.extractResource(src, dst)
    val dir = new File(dst)
    assert(dir.list().nonEmpty)
  }

  it("copyResourceToDirectory can extract a package in file system") {
    val src = SpookyUtils.getCPResource("com/tribbloids/spookystuff/utils").get
    val dst = "temp/utils/"
    SpookyUtils.extractResource(src, dst)
    val dir = new File(dst)
    assert(dir.list().nonEmpty)
  }

  it("withDeadline can write heartbeat info into log by default") {

    val (_, time) = CommonUtils.timer {
      TestHelper.intercept[TimeoutException] {
        CommonUtils.withDeadline(10.seconds, Some(1.second))(
          {
            Thread.sleep(20000)
          }
        )
      }
    }
    TestHelper.assert(time < 12000)

    val (_, time2) = CommonUtils.timer {
      CommonUtils.withDeadline(10.seconds, Some(1.second))(
        {
          Thread.sleep(5000)
        }
      )
    }
    assert(time2 < 6000)
  }

  it("withDeadline can execute heartbeat") {

    var log = ArrayBuffer[String]()

    val (_, time) = CommonUtils.timer {
      TestHelper.intercept[TimeoutException] {
        CommonUtils.withDeadline(10.seconds, Some(1.second))(
          {
            Thread.sleep(20000)
          },
          Some {
            i: Int =>
              val str = s"heartbeat: i=$i"
              println(str)
              log += str
          }
        )
      }
    }
    TestHelper.assert(time < 12000)
    log.mkString("\n").shouldBe(
      """
        |heartbeat: i=0
        |heartbeat: i=1
        |heartbeat: i=2
        |heartbeat: i=3
        |heartbeat: i=4
        |heartbeat: i=5
        |heartbeat: i=6
        |heartbeat: i=7
        |heartbeat: i=8
        |heartbeat: i=9
      """.stripMargin
    )

    log.clear()
    val (_, time2) = CommonUtils.timer {
      CommonUtils.withDeadline(10.seconds, Some(1.second))(
        {
          Thread.sleep(5000)
        },
        Some {
          i: Int =>
            val str = s"heartbeat: i=$i"
            println(str)
            log += str
        }
      )
    }
    TestHelper.assert(time2 < 6000)
    log.mkString("\n").shouldBe(
      """
        |heartbeat: i=0
        |heartbeat: i=1
        |heartbeat: i=2
        |heartbeat: i=3
        |heartbeat: i=4
      """.stripMargin
    )
  }

  it("withDeadline won't be affected by scala concurrency global ForkJoin thread pool") {

    TestHelper.TestSC.uuidSeed().mapOncePerCore {
      _ =>
        println("partition-" + TaskContext.get().partitionId())
        val (_, time) = CommonUtils.timer {
          TestHelper.intercept[TimeoutException] {
            CommonUtils.withDeadline(10.seconds, Some(1.second)) {
              Thread.sleep(20000)
              println("result 1")
            }
          }
        }
        TestHelper.assert(time < 11000, s"$time vs 11000")

        val (_, time2) = CommonUtils.timer {
          CommonUtils.withDeadline(10.seconds, Some(1.second)) {
            Thread.sleep(3000)
            println("result 2")
          }
        }
        TestHelper.assert(time2 < 6000, s"$time2 vs 6000")
    }
  }

  it("RDDs.batchReduce yield the same results as RDDs.map(_.reduce)") {
    val src = TestHelper.TestSC.parallelize(1 to 10)
    val rdds: Seq[RDD[Int]] = (1 to 10).map {
      i =>
        val result = src.map {
          j =>
            Random.nextInt(100)
        }
        result.persist()
    }

    val sum1 = rdds.zipWithIndex.map {
      case (rdd, i) =>
        rdd.reduce(_ + _)
    }

    val sum2 = SpookyUtils.RDDs.batchReduce(rdds)(_ + _)
    val sum3 = SpookyUtils.RDDs.batchReduce(rdds)(_ + _)

    assert(sum1 == sum2)
    assert(sum3 == sum1)
  }

  it("RDDs.shufflePartitions can move data into random partitions") {

    val src = TestHelper.TestSC.parallelize(1 to 100).persist()

    val shuffled1 = src.shufflePartitions
    val shuffled2 = src.shufflePartitions

    val identical = shuffled1.zipPartitions(shuffled2){
      (i1, i2) =>
        Iterator(i1.toSet == i2.toSet)
    }
      .collect()

    assert(identical.length > identical.count(identity))

    val clusters1 = shuffled1.collectPerPartition.toSet
    val clusters2 = shuffled2.collectPerPartition.toSet
    assert(clusters1 != clusters2)
  }
}