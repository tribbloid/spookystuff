package com.tribbloids.spookystuff.utils

import org.scalatest.{BeforeAndAfterEach, FunSpec}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class CachingUtilsSuite extends FunSpec with BeforeAndAfterEach {

  import CachingUtilsSuite._
  implicit def global: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

  override def beforeEach(): Unit = {
    count = 0
  }

  override def afterEach(): Unit = {
    count = 0
  }

  def createHasFinalize(): Unit = {
    HasFinalize()
  }

  describe("spike") {
    it("exit from a subroutine allows all referenced objected to be GC'ed") {

      createHasFinalize()

      //      Thread.sleep(10000)
      System.gc()
      Thread.sleep(2000)

      assert(count == 1)
    }

    it("termination of thread allows all referenced objected to be GC'ed") {

      val f: Future[Unit] = Future {

        val v1 = HasFinalize()
      }
      Await.result(f, Duration.Inf)

      //      Thread.sleep(10000)
      System.gc()
      Thread.sleep(2000)

      assert(count == 1)
    }
  }

  describe("ConcurrentCache") {

    it("has weak reference to values, allowing them to be GC'ed") {

      val cache = CachingUtils.ConcurrentCache[String, AnyRef]()

      val f: Future[Unit] = Future {

        val v1 = HasFinalize()

        cache += "a" -> v1
      }
      Await.result(f, Duration.Inf)

      System.gc()
      Thread.sleep(2000)

      assert(count == 1)
    }
  }
}

object CachingUtilsSuite {

  @volatile var count = 0

  case class HasFinalize() {

    override def finalize(): Unit = {

      count += 1
//      println("cleaned")
    }
  }

}
