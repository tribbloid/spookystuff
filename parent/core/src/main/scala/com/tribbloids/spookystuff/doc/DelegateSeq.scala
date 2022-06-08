package com.tribbloids.spookystuff.doc

import scala.language.implicitConversions

trait DelegateSeq[+T] extends Seq[T] {

  def seq: Seq[T]

  override def length: Int = seq.length

  override def iterator: Iterator[T] = seq.iterator

  override def apply(idx: Int): T = seq.apply(idx)
}

object DelegateSeq {

  implicit def unbox[T](v: DelegateSeq[T]): Seq[T] = v.seq
}
