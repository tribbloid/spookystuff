package com.tribbloids.spookystuff.actions

import scala.language.implicitConversions

object TraceAPI {

  implicit def unbox(v: TraceAPI): Trace = v.asTrace
  implicit def unboxView(v: TraceAPI): TraceView = v.traceView

  implicit def fromTrace(v: Trace): TraceView = TraceView(v)
}

trait TraceAPI extends TraceSetAPI {

  def asTrace: Trace
  @transient final lazy val traceView = TraceView(asTrace)

  @transient final lazy val asTraceSet: Set[Trace] = Set(asTrace)

  // many-to-one
  def +>(another: Action): TraceView = TraceView(asTrace :+ another)
  def +>(others: Trace): TraceView = TraceView(asTrace ++ others)
}
