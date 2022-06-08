package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

object FutureInterruptable {

  def apply[T](f: => T)(
      implicit @deprecatedName('execctx) executor: ExecutionContext
  ): FutureInterruptable[T] = new FutureInterruptable(() => f, executor)

  implicit def unbox[T](v: FutureInterruptable[T]): Future[T] = v.future
}

class FutureInterruptable[T](
    f: () => T,
    val executor: ExecutionContext
) extends LocalCleanable {

  @volatile var thread: Thread = _

  val future: Future[T] = Future {
    thread = Thread.currentThread()
    f()
  }(executor)

  def interrupt(): Unit = {

    thread.interrupt()
  }

  override def cleanImpl(): Unit = interrupt()
}
