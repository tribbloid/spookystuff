package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.testutils.AssertSerializable
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.{HashPartitioner, TaskContext}

/**
  * Created by peng on 16/11/16.
  */
class CleanableSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  it("Lifespan is serializable") {

    sc.foreachExecutorCore {
      val lifespan = Lifespan.Task()

      AssertSerializable(lifespan)
    }

    assertSerializable(Lifespan.JVM())
  }

  it("Lifespan._id should be updated after being shipped to a different executor") {

    val rdd = sc.mapPerExecutorCore {
      val lifespan = Lifespan.Task()
      val oldID = lifespan._id.asInstanceOf[Lifespan.Task.ID].id
      lifespan -> oldID
    }

    val repartitioned = rdd
      .partitionBy(new HashPartitioner(4))
    assert(repartitioned.partitions.length == 4)

    repartitioned
      .map {
        tuple =>
          val v = tuple._1
          val newID = TaskContext.get().taskAttemptId()
          Predef.assert (v._id.asInstanceOf[Lifespan.Task.ID].id == newID)
          tuple._2 -> newID
      }
      .collectPerPartition
      .foreach {
        println
      }
  }

  it("Lifespan._id should be updated after being shipped to driver") {

    val rdd = sc.mapPerWorker {
      val lifespan = Lifespan.Auto()
      val oldID = lifespan._id.asInstanceOf[Lifespan.Task.ID].id
      lifespan -> oldID
    }

    val collected = rdd.collect()

    collected
      .foreach {
        tuple =>
          val v = tuple._1
          Predef.assert (v._id.isInstanceOf[Lifespan.JVM.ID])
      }
  }

  it("Lifespan._id should be updated after being shipped to a new thread created by a different executor") {
    import scala.concurrent.duration._

    val rdd = sc.mapPerExecutorCore {
      val lifespan = Lifespan.Task()
      val oldID = lifespan._id.asInstanceOf[Lifespan.Task.ID].id
      lifespan -> oldID
    }

    val repartitioned = rdd
      .partitionBy(new HashPartitioner(4))
    assert(repartitioned.partitions.length == 4)

    repartitioned
      .map {
        tuple =>
          val v: Lifespan = tuple._1
          val newID = TaskContext.get().taskAttemptId()
//          val newID2 = v._id
          val newID3 = SpookyUtils.withDeadline(10.seconds) {
            val result = v._id
//            Predef.assert(v._id == newID2)
            result
          }
          Predef.assert (newID3.asInstanceOf[Lifespan.Task.ID].id == newID)
          tuple._2 -> newID
      }
      .collectPerPartition
      .foreach {
        println
      }
  }
}
