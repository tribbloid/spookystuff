package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.lifespan.ThreadLocal
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

case class Stopwatch() {

  @volatile var _name: String = _
  def nameOpt: Option[String] = Option(_name)

  @volatile var start: Long = _

  lazy val splitHistory: ArrayBuffer[Long] = ArrayBuffer.empty

  {
    reset()
  }

  def reset(nameOpt: Option[String] = None): Unit = this.synchronized {
    _name = nameOpt.orNull
    start = System.currentTimeMillis()
    splitHistory.clear()
  }

  def lastSplitOpt: Option[Long] = splitHistory.lastOption

  def lastSplit: Long = {
    lastSplitOpt.getOrElse {
      split
      lastSplitOpt.get
    }
  }

  def split: Long = this.synchronized {

    val result = System.currentTimeMillis() - start
    splitHistory += result
    result
  }

  def apply[T](fn: => T): Run[T] = new Run(fn)

  class Run[T](fn: => T) {

    def outer: Stopwatch = Stopwatch.this

    val result: T = {
      outer.reset()
      val result = fn
      outer.split
      result
    }

    def logDuration(
        logger: Stopwatch => Unit = { v =>
          LoggerFactory.getLogger(this.getClass).info(s"${nameOpt.getOrElse("???")} takes ${v.lastSplit}ms")
        }
    ): T = {
      logger(outer)
      result
    }

    def exportAs[R](
        fn: Stopwatch => R
    ): (T, R) = {
      result -> fn(outer)
    }
  }
}

object Stopwatch {

  val existing: ThreadLocal[Stopwatch] = ThreadLocal(_ => Stopwatch())

  def get: Stopwatch = existing.get()

  def remove(): Unit = existing.remove()

}
