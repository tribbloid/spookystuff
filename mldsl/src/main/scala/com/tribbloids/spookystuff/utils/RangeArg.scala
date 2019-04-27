package com.tribbloids.spookystuff.utils

import org.apache.commons.lang.math

import scala.collection.immutable.NumericRange
import scala.language.implicitConversions

case class RangeArg(
    delegate: math.LongRange
) extends IDMixin {
  override def _id: Any = delegate

  override lazy val toString: String = {
    val startOpt = Option(start).filter(_ != Long.MinValue)
    val endOpt = Option(end).filter(_ != Long.MaxValue)

    Seq(startOpt, endOpt)
      .map { v =>
        v.getOrElse(".")
      }
      .distinct
      .mkString("[", "..", "]")
  }

  lazy val start = delegate.getMinimumLong
  lazy val end = delegate.getMaximumLong
}

object RangeArg {

  implicit def fromRange(v: Range): RangeArg = {
    assert(v.step == 1, "Range with step != 1 is not supported")
    RangeArg(new math.LongRange(v.start, v.end))
  }

  implicit def fromNumericRange[T](v: NumericRange[T])(implicit ev: T => Number): RangeArg = {
    assert(v.step == 1, "Range with step != 1 is not supported")
    val end = ev(v.end).longValue()
    val last = if (v.isInclusive) end else end - 1L
    RangeArg(new math.LongRange(v.start, last))
  }

  implicit def fromApacheRange(v: math.Range): RangeArg = {
    v match {
      case vv: math.LongRange => RangeArg(vv)
      case _                  => RangeArg(new math.LongRange(v.getMinimumLong, v.getMaximumLong))
    }
  }
}
