package com.tribbloids.spookystuff.utils

import java.util.concurrent.LinkedBlockingDeque

import scala.language.implicitConversions

case class CircularDeque[T](size: Int = 10) {

  import scala.jdk.CollectionConverters._

  lazy val delegate: LinkedBlockingDeque[T] = new LinkedBlockingDeque[T](size)

  def forceAddFirst(v: T): Unit = delegate.synchronized {

    var success: Boolean = false

    while (!success) {

      try {
        delegate.addFirst(v)
        success = true
      } catch {
        case _: IllegalStateException =>
          delegate.removeLast()
      }
    }
  }

  def forceAddLast(v: T): Unit = delegate.synchronized {

    var success: Boolean = false

    while (!success) {

      try {
        delegate.addLast(v)
        success = true
      } catch {
        case _: IllegalStateException =>
          delegate.removeFirst()
      }
    }
  }

  def toList: List[T] = delegate.asScala.toList

}

object CircularDeque {

  implicit def toDelegate[T](v: CircularDeque[T]): LinkedBlockingDeque[T] = v.delegate
}
