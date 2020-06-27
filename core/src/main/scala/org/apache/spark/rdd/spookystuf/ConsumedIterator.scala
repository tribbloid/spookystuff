package org.apache.spark.rdd.spookystuf

import java.util.concurrent.atomic.AtomicInteger

import scala.language.implicitConversions

trait ConsumedIterator {
  self: Iterator[_] =>

  /**
    * pointer to the next value
    */
  def offset: Int
}

object ConsumedIterator {

  case class Wrap[+T](delegate: Iterator[T], initialOffset: Int = 0) extends Iterator[T] with ConsumedIterator {

    val _offset: AtomicInteger = new AtomicInteger(initialOffset)
    override def offset: Int = _offset.get()

    override def hasNext: Boolean = delegate.hasNext

    override def next(): T = {
      val result = delegate.next()
      _offset.incrementAndGet()
      result
    }
  }

  implicit def wrap[T](delegate: Iterator[T]): Wrap[T] = Wrap(delegate)

  object empty extends Wrap[Nothing](Iterator.empty)
}
