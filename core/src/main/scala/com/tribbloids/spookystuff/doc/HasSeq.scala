package com.tribbloids.spookystuff.doc

import scala.language.implicitConversions

trait HasSeq[+T] {

  def seq: Seq[T]
}

object HasSeq {

  implicit def unbox[T](v: HasSeq[T]): Seq[T] = v.seq
}
