package com.tribbloids.spookystuff.utils

import scala.collection.immutable.NumericRange
import scala.language.implicitConversions

case class RangeMagnet(
    start: Long,
    last: Long
) {
  // much longer than ordinary scala range

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

object RangeMagnet {

  implicit def fromRange(v: Range): RangeMagnet = {
    assert(v.step == 1, "Range with step != 1 is not supported")
    RangeMagnet(v.start.toLong, v.last.toLong)
  }

  implicit def fromAnyNumericRange[T](v: NumericRange[T])(
      implicit
      ev: T => Number
  ): RangeMagnet = {
    assert(v.step.intValue() == 1, "Range with step != 1 is not supported")
    val end = ev(v.end).longValue()
    val last = if (v.isInclusive) end else end - 1L
    RangeMagnet(v.start.longValue(), last.longValue())
  }

  val zero: RangeMagnet = RangeMagnet(0L, 0L)
  val maxLength: RangeMagnet = RangeMagnet(0L, Long.MaxValue)
}
