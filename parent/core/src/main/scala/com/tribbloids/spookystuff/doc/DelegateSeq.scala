package com.tribbloids.spookystuff.doc

import scala.language.implicitConversions

trait DelegateSeq[+T] extends Seq[T] {

  def delegate: Seq[T]

  override def length: Int = delegate.length

  override def iterator: Iterator[T] = delegate.iterator

  override def apply(idx: Int): T = delegate.apply(idx)
}

object DelegateSeq {

  implicit def unbox[T](v: DelegateSeq[T]): Seq[T] = v.delegate
}
