package com.tribbloids.spookystuff.lifespan

import org.scalatest.funspec.AnyFunSpec

import java.lang.ref.Cleaner
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.ref.{PhantomReference, ReferenceQueue, WeakReference}

class GCCleaningSpike extends AnyFunSpec {

  import GCCleaningSpike._

  describe("System.gc() can dispose unreachable object") {

    it("with finalizer") {

      var v = Dummies._1()

      assertInc {
        v = null
      }
    }

    it("<: class with finalizer") {

      var v = Dummies._2()

      assertInc {
        v = null
      }
    }

    // TODO: the following doesn't work, why?
    ignore("registered to a cleaner") {

      @volatile var v = Dummies._3()

      assertInc {
        v = null
      }
    }

    ignore("registered to a phantom reference cleanup thread") {

      @volatile var v = Dummies._4()

      assertInc {
        v = null
      }
    }

    ignore("registered to a weak reference cleanup thread") {

      @volatile var v = Dummies._4()

      assertInc {
        v = null
      }
    }
  }
}

object GCCleaningSpike {

  implicit lazy val ec: ExecutionContextExecutor = ExecutionContext.global

  case class WithFinalizer(fn: () => Unit) {

    case class _1() {
      override def finalize(): Unit = fn()
    }

    trait _2Base {
      override def finalize(): Unit = fn()
    }
    case class _2() extends _2Base

    final val _cleaner = Cleaner.create()
    case class _3() extends AutoCloseable {

      final private val cleanable = _cleaner.register(
        this,
        { () =>
          println("\ncleaned\n")
          fn()
        }
      )

      override def close(): Unit = cleanable.clean()
    }

    lazy val phantomQueue = new ReferenceQueue[_2Base]()
    case class _4() {
      val ref = new PhantomReference(this, phantomQueue)
    }

    val cleaningPhantom: Future[Unit] = Future {
      while (true) {
        phantomQueue.remove
        fn()
      }
    }

    lazy val weakQueue = new ReferenceQueue[_2Base]()
    case class _5() {
      val ref = new WeakReference(this, weakQueue)
    }

    val cleaningWeak: Future[Unit] = Future {
      while (true) {
        weakQueue.remove
        fn()
      }
    }
  }

  @transient var count = 0

  val doInc: () => Unit = () => count += 1

  def assertInc(fn: => Unit): Unit = {
    val c1 = count

    fn

    System.gc()

    Thread.sleep(1000)
    val c2 = count

    assert(c2 - c1 == 1)
  }

  object Dummies extends WithFinalizer(doInc)
}
