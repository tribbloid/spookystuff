package com.tribbloids.spookystuff.lifespan

import ai.acyclic.prover.commons.spark.serialization.AssertSerializable
import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.commons.lifespan.{Cleanable, LeafType, LocalCleanable}
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import org.apache.spark.{HashPartitioner, SparkException, TaskContext}

import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

// TODO: move to previous module
/**
  * Created by peng on 16/11/16.
  */
class CleanableSuite extends SpookyBaseSpec {

  import com.tribbloids.spookystuff.lifespan.CleanableSuite.*
  import com.tribbloids.spookystuff.utils.RDDImplicits.*

  override def beforeEach(): Unit = {
    super.beforeEach()
    sc.runEverywhere(alsoOnDriver = false) { _ =>
      Cleanable.All
        .filter {
          case _: DummyCleanable => true
          case _                 => false
        }
        .cleanSweep()
    }
  }

  it("Lifespan.JVM.batchID is serializable") {
    val v1 = Lifespan.JVM().batchID
    val v2 = v1
    assert(v1 == v2)

    AssertSerializable(v1)
  }

  it("Lifespan.JVM is serializable") {

    lifespanIsSerializable(Lifespan.JVM())

    val rdd = sc.uuidSeed().map { _ =>
      val lifespan = Lifespan.JVM()
      lifespanIsSerializable(lifespan)
      lifespan
    }
    rdd.count()
    rdd.collect()
  }

  it("Lifespan.Task is serializable") {

    val rdd = sc.uuidSeed().map { _ =>
      val lifespan = Lifespan.Task()
      lifespanIsSerializable(lifespan)
      lifespan
    }
    rdd.count()
    intercept[SparkException] { // cannot be re-initialized outside Task
      val vv = rdd.collect()
      vv
    }
  }

  it("Lifespan.batchIDs should be updated after being shipped to a different executor") {

    val rdd = sc.uuidSeed().mapOncePerCore { _ =>
      val lifespan = Lifespan.Task()
      val oldID = lifespan.registeredIDs.head.asInstanceOf[Lifespan.Task.ID]
      lifespan -> oldID
    }

    val repartitioned = rdd
      .partitionBy(new HashPartitioner(4))
    assert(repartitioned.partitions.length == 4)

    val old_new = repartitioned.map { tuple =>
      val v = tuple._1
      val newID = TaskContext.get().taskAttemptId()
      Predef.assert(v.registeredIDs.head.asInstanceOf[Lifespan.Task.ID] == newID)
      tuple._2 -> newID
    }.collectPerPartition

    val difference = old_new.flatten.count(v => v._1 != v._2)
    require(difference > 0)
  }

  it("Lifespan._id should be updated after being shipped to driver") {

    val rdd = sc.uuidSeed().mapOncePerWorker { _ =>
      val lifespan = Lifespan.TaskOrJVM()
      val oldID = lifespan.registeredIDs.head.asInstanceOf[Lifespan.Task.ID]
      lifespan -> oldID
    }

    val collected = rdd.collect()

    collected
      .foreach { tuple =>
        val v = tuple._1
        Predef.assert(v.registeredIDs.head.isInstanceOf[Lifespan.JVM.ID])
      }
  }

  it("Lifespan.batchIDs should be updated after being shipped to a new thread created by a different executor") {
    import scala.concurrent.duration.*

    val rdd = sc.uuidSeed().mapOncePerCore { _ =>
      val lifespan = Lifespan.Task()
      val oldID = lifespan.registeredIDs.head.asInstanceOf[Lifespan.Task.ID]
      lifespan -> oldID
    }

    val repartitioned = rdd
      .partitionBy(new HashPartitioner(4))
    assert(repartitioned.partitions.length == 4)

    val old_new = repartitioned.map { tuple =>
      val v: Lifespan = tuple._1
      val newID = TaskContext.get().taskAttemptId()
      //          val newID2 = v._id
      val newID3 = CommonUtils.withTimeout(10.seconds) {
        val result = v.registeredIDs.head
        //            Predef.assert(v._id == newID2)
        result
      }
      Predef.assert(newID3.asInstanceOf[Lifespan.Task.ID] == newID)
      tuple._2 -> newID
    }.collectPerPartition

    val flatten = old_new.toList.flatten

    assert(flatten.count(v => v._1 == v._2) < flatten.size)
  }

  it("can get all created Cleanables") {

    verify(i => DummyCleanable(i))
  }

  private def verify(getDummy: Int => Unit) = {
    val ss = 1 to 10

    val inc1 = sc
      .uuidSeed()
      .mapOncePerWorker { _ =>
        counter.get()
      }
      .reduce(_ + _)

    for (j <- 1 to 10) {
      sc.parallelize(ss).foreach { i =>
        getDummy(i)
      }
    }

    val inc2 = sc
      .uuidSeed()
      .mapOncePerWorker { _ =>
        Cleanable.All.typed[DummyCleanable].cleanSweep()
        counter.get()
      }
      .reduce(_ + _)

    sc
      .uuidSeed()
      .mapOncePerWorker { _ =>
        Cleanable.All.typed[DummyCleanable].cleanSweep()
        counter.get()
      }
      .reduce(_ + _)

    assert(inc2 - inc1 == ss.size * 10)
//    assert(i2.distinct.sorted == ss)
  }

  it("can get all created Cleanables even their hashcodes may overlap") {

    verify(i => DummyCleanable(i, None))
  }

}

object CleanableSuite {

  val counter: AtomicInteger = new AtomicInteger(0)

  val doInc: () => Unit = () => counter.incrementAndGet()

  def incOf(fn: => Unit): Int = {
    val c1 = counter.get()

    fn

    System.gc()

    Thread.sleep(1000)
    val c2 = counter.get()

    c2 - c1
  }

  def assertInc(fn: => Unit, expected: Int = 1): Unit = {
    val inc = incOf(fn)

    assert(inc == expected)
  }

  case class DummyCleanable(
      index: Int,
      id: Option[Long] = Some(Random.nextLong())
  ) extends LocalCleanable {

    override protected def cleanImpl(): Unit = {
      counter.incrementAndGet()
    }
  }

  def lifespanIsSerializable(v: Lifespan): Unit = {

    AssertSerializable[Lifespan](v).on(
      condition = { (v1, v2) =>
        AssertSerializable.strongCondition(v1, v2)
        Seq(v1, v2).foreach {
          case v: LeafType#Internal =>
            v.requireUsable()
          case _ =>
        }
      }
    )
  }
}
