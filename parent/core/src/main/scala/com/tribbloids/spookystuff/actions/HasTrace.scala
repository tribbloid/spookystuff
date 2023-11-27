package com.tribbloids.spookystuff.actions

import scala.language.implicitConversions

trait HasTrace extends HasTraceSet {

  def trace: Trace

  @transient final lazy val asTraceSet: TraceSet = TraceSet.of(trace)

  // many-to-one
  def +>(another: Action): Trace = Trace(trace :+ another)
  def +>(others: Trace): Trace = Trace(trace ++ others)
}

object HasTrace {

  implicit def unbox(v: HasTrace): Trace = v.trace
}
