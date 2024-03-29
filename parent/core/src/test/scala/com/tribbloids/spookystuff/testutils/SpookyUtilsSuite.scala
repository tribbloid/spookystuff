package com.tribbloids.spookystuff.testutils

import com.tribbloids.spookystuff.utils.{AwaitWithHeartbeat, CommonUtils, SpookyUtils, SpookyViews}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.TimeoutException
import scala.util.Random

/**
  * Created by peng on 11/1/14.
  */
class SpookyUtilsSuite extends BaseSpec {

  import SpookyViews._

  import scala.concurrent.duration._

  it("canonizeUrn should clean ?:$&#") {
    val url = SpookyUtils.canonizeUrn("http://abc.com?re#k2$si")
    assert(url === "http/abc.com/re/k2/si")
  }

  it("asArray[Int]") {
    assert(SpookyUtils.asArray[Int](2).toSeq == Seq(2))
    assert(SpookyUtils.asArray[Int](Seq(1, 2, 3).iterator).toSeq == Seq(1, 2, 3))
    assert(SpookyUtils.asArray[Int](Seq(1, 2.2, "b")).toSeq == Seq(1))
  }

  it("asIterable[Int]") {
    assert(SpookyUtils.asIterable[Int](2) == Iterable(2))
    assert(SpookyUtils.asIterable[Int](Seq(1, 2, 3).iterator).toSeq == Iterable(1, 2, 3))
    assert(SpookyUtils.asIterable[Int](Seq(1, 2.2, "b")).toSeq == Iterable(1))
  }

  describe("withTimeout") {

    it("can write heartbeat info into log by default") {

      val (_, time) = CommonUtils.timed {
        TestHelper.intercept[TimeoutException] {
          CommonUtils.withTimeout(10.seconds, 1.second) {
            Thread.sleep(20000)
          }
        }
      }
      assert(time < 14000)
      // TODO: the delay of 4s is too long, should tighten after switching to lightweight cats-effect fibre

      val (_, time2) = CommonUtils.timed {
        CommonUtils.withTimeout(10.seconds, 1.second) {
          Thread.sleep(5000)
        }
      }
      assert(time2 < 6000)
    }

    it("can execute heartbeat") {

      val log = ArrayBuffer[Int]()

      val (_, time) = CommonUtils.timed {
        TestHelper.intercept[TimeoutException] {
          CommonUtils.withTimeout(10.seconds, 1.second)(
            Thread.sleep(20000),
            AwaitWithHeartbeat.Heartbeat.WrapWithInfo { i: Int =>
              log += i
              true
            }
          )
        }
      }
      Predef.assert(time < 12000)
      Predef.assert((8 to 10).contains(log.max))

      log.clear()
      val (_, time2) = CommonUtils.timed {
        CommonUtils.withTimeout(10.seconds, 1.second)(
          Thread.sleep(5000),
          AwaitWithHeartbeat.Heartbeat.WrapWithInfo { i: Int =>
            log += i
            true
          }
        )
      }
      Predef.assert(time2 < 6000)
      Predef.assert((4 to 5).contains(log.max))
    }

    it("won't be affected by scala concurrency global ForkJoin thread pool") {

      TestHelper.TestSC.uuidSeed().mapOncePerCore { _ =>
        println("partition-" + TaskContext.get().partitionId())
        val (_, time) = CommonUtils.timed {
          TestHelper.intercept[TimeoutException] {
            CommonUtils.withTimeout(10.seconds, 1.second) {
              Thread.sleep(20000)
              println("result 1")
            }
          }
        }
        Predef.assert(time < 11000, s"$time vs 11000")

        val (_, time2) = CommonUtils.timed {
          CommonUtils.withTimeout(10.seconds, 1.second) {
            Thread.sleep(3000)
            println("result 2")
          }
        }
        Predef.assert(time2 < 6000, s"$time2 vs 6000")
      }
    }
  }

  it("RDDs.batchReduce yield the same results as RDDs.map(_.reduce)") {
    val src = TestHelper.TestSC.parallelize(1 to 10)
    val rdds: Seq[RDD[Int]] = (1 to 10).map { _ =>
      val result = src.map { _ =>
        Random.nextInt(100)
      }
      result.persist()
    }

    val sum1 = rdds.zipWithIndex.map {
      case (rdd, _) =>
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

    val identical = shuffled1
      .zipPartitions(shuffled2) { (i1, i2) =>
        Iterator(i1.toSet == i2.toSet)
      }
      .collect()

    assert(identical.length > identical.count(identity))

    val clusters1 = shuffled1.collectPerPartition.toSet
    val clusters2 = shuffled2.collectPerPartition.toSet
    assert(clusters1 != clusters2)
  }
}
