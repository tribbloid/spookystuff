package com.tribbloids.spookystuff.utils

import java.io.File

import com.tribbloids.spookystuff.testutils.{TestHelper, TestMixin}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.TimeoutException
import scala.util.Random

/**
  * Created by peng on 11/1/14.
  */
class SpookyUtilsSuite extends FunSuite with TestMixin {

  import SpookyViews._
  import scala.concurrent.duration._

  test("canonizeUrn should clean ?:$&#"){
    val url = SpookyUtils.canonizeUrn("http://abc.com?re#k2$si")
    assert(url === "http/abc.com/re/k2/si")
  }

  test("asArray[Int]") {
    assert(SpookyUtils.asArray[Int](2).toSeq == Seq(2))
    assert(SpookyUtils.asArray[Int](Seq(1,2,3).iterator).toSeq == Seq(1,2,3))
    assert(SpookyUtils.asArray[Int](Seq(1, 2.2, "b")).toSeq == Seq(1))
  }


  test("asIterable[Int]") {
    assert(SpookyUtils.asIterable[Int](2) == Iterable(2))
    assert(SpookyUtils.asIterable[Int](Seq(1,2,3).iterator).toSeq == Iterable(1,2,3))
    assert(SpookyUtils.asIterable[Int](Seq(1, 2.2, "b")).toSeq == Iterable(1))
  }

  test("copyResourceToDirectory can extract a dependency's package in a jar") {
    val src = SpookyUtils.getCPResource("org/apache/log4j/xml").get
    val dst = SpookyUtils.\\\(TestHelper.TEMP_PATH, "log4j")
    SpookyUtils.extractResource(src, dst)
    val dir = new File(dst)
    assert(dir.list().nonEmpty)
  }

  test("copyResourceToDirectory can extract a package in file system") {
    val src = SpookyUtils.getCPResource("com/tribbloids/spookystuff/utils").get
    val dst = "temp/utils/"
    SpookyUtils.extractResource(src, dst)
    val dir = new File(dst)
    assert(dir.list().nonEmpty)
  }

  test("withDeadline can write heartbeat info into log by default") {

    val (_, time) = TestHelper.timer {
      TestHelper.intercept[TimeoutException] {
        SpookyUtils.withDeadline(10.seconds, Some(1.second))(
          {
            Thread.sleep(20000)
          }
        )
      }
    }
    TestHelper.assert(time < 12000)

    val (_, time2) = TestHelper.timer {
      SpookyUtils.withDeadline(10.seconds, Some(1.second))(
        {
          Thread.sleep(5000)
        }
      )
    }
    TestHelper.assert(time2 < 6000)
  }

  test("withDeadline can execute heartbeat") {

    var log = ArrayBuffer[String]()

    val (_, time) = TestHelper.timer {
      TestHelper.intercept[TimeoutException] {
        SpookyUtils.withDeadline(10.seconds, Some(1.second))(
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
    val (_, time2) = TestHelper.timer {
      SpookyUtils.withDeadline(10.seconds, Some(1.second))(
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

  test("withDeadline won't be affected by scala concurrency global ForkJoin thread pool") {

    TestHelper.TestSpark.foreachExecutorCore {

      println("partition-" + TaskContext.get().partitionId())
      val (_, time) = TestHelper.timer {
        TestHelper.intercept[TimeoutException] {
          SpookyUtils.withDeadline(10.seconds, Some(1.second)) {
            Thread.sleep(20000)
          }
        }
      }
      TestHelper.assert(time < 11000)

      val (_, time2) = TestHelper.timer {
        SpookyUtils.withDeadline(10.seconds, Some(1.second)) {
          Thread.sleep(5000)
        }
      }
      TestHelper.assert(time2 < 6000)
    }
  }

  test("RDDs.batchReduce yield the same results as RDDs.map(_.reduce)") {
    val src = TestHelper.TestSpark.parallelize(1 to 10)
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

  test("RDDs.shuffle can move data into random partitions") {

    val src = TestHelper.TestSpark.parallelize(1 to 100).persist()

    val shuffled1 = SpookyUtils.RDDs.shuffle(src)
    val shuffled2 = SpookyUtils.RDDs.shuffle(src)

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