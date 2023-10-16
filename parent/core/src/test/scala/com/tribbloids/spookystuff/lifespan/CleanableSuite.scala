package com.tribbloids.spookystuff.lifespan

import com.tribbloids.spookystuff.testutils.SparkEnvSpec
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, LeafType, LocalCleanable}
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import org.scalatest.BeforeAndAfterEach

import scala.util.Random

// TODO: move to previous module
/**
  * Created by peng on 16/11/16.
  */
class CleanableSuite extends SparkEnvSpec with BeforeAndAfterEach {

  import com.tribbloids.spookystuff.lifespan.CleanableSuite._
  import com.tribbloids.spookystuff.utils.SpookyViews._

  override def beforeEach(): Unit = {
    super.beforeEach()
    sc.runEverywhere(alsoOnDriver = false) { _ =>
      Cleanable.All.cleanSweep {
        case _: DummyCleanable => true
        case _                 => false
      }
    }
  }

//  it("Lifespan.JVM.batchID is serializable") {
//    val v1 = Lifespan.JVM().batchID
//    val v2 = v1.copy(v1.id)
//    assert(v1 == v2)
//
//    AssertSerializable(v1)
//  }
//
//  it("Lifespan.JVM is serializable") {
//
//    lifespanIsSerializable(Lifespan.JVM())
//
//    val rdd = sc.uuidSeed().map { _ =>
//      val lifespan = Lifespan.JVM()
//      lifespanIsSerializable(lifespan)
//      lifespan
//    }
//    rdd.count()
//    rdd.collect()
//  }
//
//  it("Lifespan.Task is serializable") {
//
//    val rdd = sc.uuidSeed().map { _ =>
//      val lifespan = Lifespan.Task()
//      lifespanIsSerializable(lifespan)
//      lifespan
//    }
//    rdd.count()
//    intercept[SparkException] { // cannot be re-initialized outside Task
//      val vv = rdd.collect()
//      vv
//    }
//  }
//
//  it("Lifespan.batchIDs should be updated after being shipped to a different executor") {
//
//    val rdd = sc.uuidSeed().mapOncePerCore { _ =>
//      val lifespan = Lifespan.Task()
//      val oldID = lifespan.registeredIDs.head.asInstanceOf[Lifespan.Task.ID].id
//      lifespan -> oldID
//    }
//
//    val repartitioned = rdd
//      .partitionBy(new HashPartitioner(4))
//    assert(repartitioned.partitions.length == 4)
//
//    val old_new = repartitioned.map { tuple =>
//      val v = tuple._1
//      val newID = TaskContext.get().taskAttemptId()
//      Predef.assert(v.registeredIDs.head.asInstanceOf[Lifespan.Task.ID].id == newID)
//      tuple._2 -> newID
//    }.collectPerPartition
//
//    val difference = old_new.flatten.count(v => v._1 != v._2)
//    require(difference > 0)
//  }
//
//  it("Lifespan._id should be updated after being shipped to driver") {
//
//    val rdd = sc.uuidSeed().mapOncePerWorker { _ =>
//      val lifespan = Lifespan.TaskOrJVM()
//      val oldID = lifespan.registeredIDs.head.asInstanceOf[Lifespan.Task.ID].id
//      lifespan -> oldID
//    }
//
//    val collected = rdd.collect()
//
//    collected
//      .foreach { tuple =>
//        val v = tuple._1
//        Predef.assert(v.registeredIDs.head.isInstanceOf[Lifespan.JVM.ID])
//      }
//  }
//
//  it("Lifespan.batchIDs should be updated after being shipped to a new thread created by a different executor") {
//    import scala.concurrent.duration._
//
//    val rdd = sc.uuidSeed().mapOncePerCore { _ =>
//      val lifespan = Lifespan.Task()
//      val oldID = lifespan.registeredIDs.head.asInstanceOf[Lifespan.Task.ID].id
//      lifespan -> oldID
//    }
//
//    val repartitioned = rdd
//      .partitionBy(new HashPartitioner(4))
//    assert(repartitioned.partitions.length == 4)
//
//    val old_new = repartitioned.map { tuple =>
//      val v: Lifespan = tuple._1
//      val newID = TaskContext.get().taskAttemptId()
//      //          val newID2 = v._id
//      val newID3 = CommonUtils.withTimeout(10.seconds) {
//        val result = v.registeredIDs.head
//        //            Predef.assert(v._id == newID2)
//        result
//      }
//      Predef.assert(newID3.asInstanceOf[Lifespan.Task.ID].id == newID)
//      tuple._2 -> newID
//    }.collectPerPartition
//
//    val flatten = old_new.toList.flatten
//
//    assert(flatten.count(v => v._1 == v._2) < flatten.size)
//  }
//
//  it("can get all created Cleanables") {
//
//    verify(i => DummyCleanable(i))
//  }
//
//  private def verify(getDummy: Int => Unit) = {
//    val ss = 1 to 10
//    for (_ <- 1 to 10) {
//      sc.parallelize(ss).foreach {
//        getDummy
//      }
//    }
//
//    val i2 = sc
//      .uuidSeed()
//      .mapOncePerWorker { _ =>
//        Cleanable.All.typed[DummyCleanable].map(_.index)
//      }
//      .flatMap(identity)
//      .collect()
//      .toSeq
//
//    assert(i2.size == ss.size * 10)
//    assert(i2.distinct.sorted == ss)
//  }
//
//  it("can get all created Cleanables even their hashcodes may overlap") {
//
//    verify(i => DummyCleanable(i, None))
//  }

  describe("System.gc() can trigger Cleanable that is") {

    it("unreachable") {
      @volatile var v = DummyCleanable(1, None)

      assertInc {
        v = null

        System.gc()
      }
    }
  }

}

object CleanableSuite {

  @volatile var count = 0

  val doInc: () => Unit = () => count += 1

  def assertInc(fn: => Unit): Unit = {
    val c1 = count

    fn

    System.gc()

    Thread.sleep(1000)
    val c2 = count

    assert(c2 - c1 == 1)
  }

  case class DummyCleanable(
      index: Int,
      id: Option[Long] = Some(Random.nextLong())
  ) extends LocalCleanable {

    override protected def cleanImpl(): Unit = {
      CleanableSuite.count += 1
    }
  }

  def lifespanIsSerializable(v: Lifespan): Unit = {

    AssertSerializable[Lifespan](
      v,
      condition = { (v1, v2) =>
        AssertSerializable.serializableCondition(v1, v2)
        Seq(v1, v2).foreach {
          case v: LeafType#Internal =>
            v.requireUsable()
          case _ =>
        }
      }
    )
  }
}
