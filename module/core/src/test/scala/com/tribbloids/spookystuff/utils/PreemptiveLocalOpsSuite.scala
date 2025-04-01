package com.tribbloids.spookystuff.utils

import ai.acyclic.prover.commons.spark.TestHelper
import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.testutils.BaseSpec
import org.apache.spark.SparkContext

class PreemptiveLocalOpsSuite extends BaseSpec {

  import RDDImplicits.*

  val sc: SparkContext = TestHelper.TestSC

  it("can be much faster than toLocalIterator") {

    val max = 80
    val delay = 100

    sc.withJob(description = "root") {

      val slowRDD = sc.parallelize(1 to max, 8).map { v =>
        Thread.sleep(delay)
        v
      }

      val (r0, t0) = CommonUtils.timed {
        sc.getLocalProperty("spark.jobGroup.id")

        slowRDD.collect().toList
      }

      val (r1, t1) = CommonUtils.timed {
        slowRDD.toLocalIterator.toList
      }

      val capacity = 4
      val (r2, t2) = CommonUtils.timed {
        PreemptiveLocalOps(capacity).ForRDD(slowRDD).toLocalIterator.toList
      }

      assert(r1 == r2)
      println(s"linear: $t1, preemptive: $t2")
      assert(t1 > t2)
      assert(t2 > max * delay / capacity)
    }

//    Thread.sleep(10000000)
  }
}
