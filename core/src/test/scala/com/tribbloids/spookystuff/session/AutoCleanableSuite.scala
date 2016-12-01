package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.testutils.AssertSerializable
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.{HashPartitioner, TaskContext}

/**
  * Created by peng on 16/11/16.
  */
class AutoCleanableSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  test("Lifespan is serializable") {

    sc.foreachExecutor {
      val lifespan = new Lifespan.Task()

      AssertSerializable(lifespan)
    }

    assertSerializable(Lifespan.JVM)
  }

  test("Lifespan._id should be updated after being shipped to a different executor") {

    val rdd = sc.mapPerExecutor {
      val lifespan = new Lifespan.Task()
      val oldID = lifespan._id.left.get
      lifespan -> oldID
    }

    val repartitioned = rdd
      .partitionBy(new HashPartitioner(4))
    assert(repartitioned.getNumPartitions == 4)

    repartitioned
      .map {
        tuple =>
          val v = tuple._1
          val newID = TaskContext.get().taskAttemptId()
          Predef.assert (v._id.left.get == newID)
          tuple._2 -> newID
      }
      .collectPerPartition
      .foreach {
        println
      }
  }

  test("Lifespan._id should be updated after being shipped to driver") {

    val rdd = sc.mapPerWorker {
      val lifespan = new Lifespan.Auto()
      val oldID = lifespan._id.left.get
      lifespan -> oldID
    }

    val collected = rdd.collect()

    collected
      .foreach {
        tuple =>
          val v = tuple._1
          Predef.assert (v._id.isRight)
      }
  }

  test("Lifespan._id should be updated after being shipped to a new thread created by a different executor") {
    import scala.concurrent.duration._

    val rdd = sc.mapPerExecutor {
      val lifespan = new Lifespan.Task()
      val oldID = lifespan._id.left.get
      lifespan -> oldID
    }

    val repartitioned = rdd
      .partitionBy(new HashPartitioner(4))
    assert(repartitioned.getNumPartitions == 4)

    repartitioned
      .map {
        tuple =>
          val v = tuple._1
          val newID = TaskContext.get().taskAttemptId()
          val newID2Opt = SpookyUtils.withDeadline(10.seconds) {
            v._id
          }
          Predef.assert (newID2Opt.left.get == newID)
          tuple._2 -> newID
      }
      .collectPerPartition
      .foreach {
        println
      }
  }
}
