package com.tribbloids.spookystuff.actions

object HasTrace {}

trait HasTrace extends HasTraceSet {

  def asTrace: Trace

  @transient final lazy val asTraceSet: TraceSet = TraceSet.of(asTrace)

  // many-to-one
  def +>(another: Action): Trace = Trace(asTrace :+ another)
  def +>(others: Trace): Trace = Trace(asTrace ++ others)
}
