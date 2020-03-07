package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.session.CleanableSuite.DummyCleanable
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.spark.{HashPartitioner, SparkException, TaskContext}

import scala.util.Random

/**
  * Created by peng on 16/11/16.
  */
class CleanableSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  override def beforeEach(): Unit = {
    super.beforeEach()
    sc.runEverywhere(alsoOnDriver = false) { _ =>
      Cleanable.cleanSweepAll {
        case _: DummyCleanable => true
        case _                 => false
      }
    }
  }

  it("Lifespan.JVM is serializable") {

    AssertSerializable(Lifespan.JVM())
    val rdd = sc.uuidSeed().map { _ =>
      val lifespan = Lifespan.JVM()
      AssertSerializable(lifespan)
      lifespan
    }
    rdd.count()
    rdd.collect()
  }

  it("Lifespan.Task is serializable") {

    val rdd = sc.uuidSeed().map { _ =>
      val lifespan = Lifespan.Task()
      AssertSerializable(lifespan)
      lifespan
    }
    rdd.count()
    intercept[SparkException] { //cannot be re-initialized outside Task
      rdd.collect()
    }
  }

  it("Lifespan.SparkApp is serializable") {

    AssertSerializable(Lifespan.SparkApp())
    intercept[SparkException] {
      sc.uuidSeed().foreach { _ =>
        val lifespan = Lifespan.SparkApp()
        AssertSerializable(lifespan)
      }
    }
  }

  it("Lifespan._id should be updated after being shipped to a different executor") {

    val rdd = sc.uuidSeed().mapOncePerCore { _ =>
      val lifespan = Lifespan.Task()
      val oldID = lifespan._id.asInstanceOf[Lifespan.Task.ID].id
      lifespan -> oldID
    }

    val repartitioned = rdd
      .partitionBy(new HashPartitioner(4))
    assert(repartitioned.partitions.length == 4)

    repartitioned
      .map { tuple =>
        val v = tuple._1
        val newID = TaskContext.get().taskAttemptId()
        Predef.assert(v._id.asInstanceOf[Lifespan.Task.ID].id == newID)
        tuple._2 -> newID
      }
      .collectPerPartition
      .foreach {
        println
      }
  }

  it("Lifespan._id should be updated after being shipped to driver") {

    val rdd = sc.uuidSeed().mapOncePerWorker { _ =>
      val lifespan = Lifespan.TaskOrJVM()
      val oldID = lifespan._id.asInstanceOf[Lifespan.Task.ID].id
      lifespan -> oldID
    }

    val collected = rdd.collect()

    collected
      .foreach { tuple =>
        val v = tuple._1
        Predef.assert(v._id.isInstanceOf[Lifespan.JVM.ID])
      }
  }

  it("Lifespan._id should be updated after being shipped to a new thread created by a different executor") {
    import scala.concurrent.duration._

    val rdd = sc.uuidSeed().mapOncePerCore { _ =>
      val lifespan = Lifespan.Task()
      val oldID = lifespan._id.asInstanceOf[Lifespan.Task.ID].id
      lifespan -> oldID
    }

    val repartitioned = rdd
      .partitionBy(new HashPartitioner(4))
    assert(repartitioned.partitions.length == 4)

    repartitioned
      .map { tuple =>
        val v: Lifespan = tuple._1
        val newID = TaskContext.get().taskAttemptId()
        //          val newID2 = v._id
        val newID3 = CommonUtils.withDeadline(10.seconds) {
          val result = v._id
          //            Predef.assert(v._id == newID2)
          result
        }
        Predef.assert(newID3.asInstanceOf[Lifespan.Task.ID].id == newID)
        tuple._2 -> newID
      }
      .collectPerPartition
      .foreach {
        println
      }
  }

  it("can get all created Cleanables") {

    runTest(i => DummyCleanable(i))
  }

  private def runTest(getDummy: Int => Unit) = {
    val ss = 1 to 10
    for (_ <- 1 to 10) {
      sc.parallelize(ss).foreach {
        getDummy
      }
    }

    val i2 = sc
      .uuidSeed()
      .mapOncePerWorker { _ =>
        Cleanable.getTyped[DummyCleanable].map(_.index)
      }
      .flatMap(identity)
      .collect()
      .toSeq

    assert(i2.size == ss.size * 10)
    assert(i2.distinct.sorted == ss)
  }

  it("can get all created Cleanables even their hashcodes may overlap") {

    runTest(i => DummyCleanable(i, None))
  }
}

object CleanableSuite {

  case class DummyCleanable(
      index: Int,
      id: Option[Long] = Some(Random.nextLong())
  ) extends LocalCleanable {

    override protected def cleanImpl(): Unit = {}
  }
}
