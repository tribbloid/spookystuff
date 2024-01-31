package com.tribbloids.spookystuff.utils

import scala.collection.immutable.NumericRange
import scala.language.implicitConversions

case class RangeArg(
    start: Long,
    last: Long
//    delegate: NumericRange[Long]
) {
//  override def _equalBy: Any = delegate

  lazy val delegate: NumericRange.Inclusive[Long] = start to last

  override lazy val toString: String = {
    val startOpt = Option(start).filter(_ != Long.MinValue)
    val endOpt = Option(last).filter(_ != Long.MaxValue)

    Seq(startOpt, endOpt)
      .map { v =>
        v.getOrElse(".")
      }
      .distinct
      .mkString("[", "..", "]")
  }

//  lazy val start: Long = delegate.start
//  lazy val last: Long = delegate.last

  def containsLong(v: Long): Boolean = {

    (start <= v) && (last >= v)
  }
}

object RangeArg {

  implicit def fromRange(v: Range): RangeArg = {
    assert(v.step == 1, "Range with step != 1 is not supported")
    RangeArg(v.start.toLong, v.last.toLong)
  }

  implicit def fromAnyNumericRange[T](v: NumericRange[T])(
      implicit
      ev: T => Number
  ): RangeArg = {
    assert(v.step.intValue() == 1, "Range with step != 1 is not supported")
    val end = ev(v.end).longValue()
    val last = if (v.isInclusive) end else end - 1L
    RangeArg(v.start.longValue(), last.longValue())
  }

}
