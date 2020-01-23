package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.spark.SparkContext
import org.scalatest.FunSpec

import scala.language.implicitConversions

class ToLocalIteratorPreemptivelySuite extends FunSpec {

  import SpookyViews._
  import ToLocalIteratorPreemptivelySuite._

  val sc: SparkContext = TestHelper.TestSC

  it("can be much faster than toLocalIterator") {

    val max = 80
    val delay = 100

    sc.withJob(description = "root") {

      val slowRDD = sc.parallelize(1 to max, 8).map { v =>
        Thread.sleep(delay)
        v
      }

      val (r0, t0) = timed {
        val jbiid = sc.getLocalProperty("spark.jobGroup.id")

        slowRDD.collect().toList
      }

      val (r1, t1) = timed {
        slowRDD.toLocalIterator.toList
      }

      val capacity = 4
      val (r2, t2) = timed {
        slowRDD.toLocalIteratorPreemptively(capacity).toList
      }

      assert(r1 == r2)
      println(s"linear: $t1, preemptive: $t2")
      assert(t1 > t2)
      assert(t2 > max * delay / capacity)
    }

//    Thread.sleep(10000000)
  }
}

object ToLocalIteratorPreemptivelySuite {

  def timed[T](fn: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val result = fn
    val endTime = System.currentTimeMillis()
    (result, endTime - startTime)
  }
}
