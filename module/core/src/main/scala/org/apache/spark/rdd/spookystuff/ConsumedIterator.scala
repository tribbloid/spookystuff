package org.apache.spark.rdd.spookystuff

import java.util.concurrent.atomic.AtomicInteger
import scala.language.implicitConversions

trait ConsumedIterator {
  self: Iterator[?] =>

  /**
    * pointer to the next value
    */
  def offset: Int

  lazy val trackingNumber: Int = System.identityHashCode(this)

  override lazy val toString: String = s"${this.getClass.getSimpleName}@${trackingNumber}"
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
