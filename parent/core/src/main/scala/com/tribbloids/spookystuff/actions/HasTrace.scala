package com.tribbloids.spookystuff.actions

import scala.language.implicitConversions

trait HasTrace extends HasTraceSet {

  def asTrace: Trace

  @transient final lazy val asTraceSet: TraceSet = TraceSet.of(asTrace)

  // many-to-one
  def +>(another: Action): Trace = Trace(asTrace :+ another)
  def +>(others: Trace): Trace = Trace(asTrace ++ others)
}

object HasTrace {

  implicit def unbox(v: HasTrace): Trace = v.asTrace
}
