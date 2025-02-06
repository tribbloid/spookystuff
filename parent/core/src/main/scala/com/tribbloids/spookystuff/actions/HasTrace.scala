package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Trace.NoOp

import scala.language.implicitConversions

trait HasTrace extends HasTraceSet {

  def asTrace: Trace

  @transient final lazy val asTraceSet: TraceSet = TraceSet.of(asTrace)

  // many-to-one
  def +>(another: Action): Trace = Trace(asTrace :+ another)
  def +>(that: Trace): Trace = {

    (this, that) match {
      case (NoOp, _) => NoOp
    }
    Trace(asTrace ++ that)
  }
}

object HasTrace {

  implicit def unbox(v: HasTrace): Trace = v.asTrace
}
