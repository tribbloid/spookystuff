package com.tribbloids.spookystuff.actions

import scala.language.implicitConversions

object TraceSet {

  implicit def unbox(v: TraceSet): Set[Trace] = v.self

  def of(ts: Trace*): TraceSet = TraceSet(ts.toSet)

  trait NonEmptyCap { self: TraceSet => }
  type NonEmpty = TraceSet with NonEmptyCap
}

/**
  * read [[ai.acyclic.prover.commons.__OperatorPrecedence]] when defining new operators
  * @param self
  *   no duplicated Trace
  */
case class TraceSet(self: Set[Trace]) extends HasTraceSet {
  import TraceSet._
  override def asTraceSet: TraceSet = this
  // many-to-one
  def +>(another: Action): TraceSet = TraceSet(self.map(trace => trace :+ another))
  def +>(others: Trace): TraceSet = TraceSet(self.map(trace => trace ++ others))

  def outputNames: Set[String] = asTraceSet.map(_.outputNames).reduce(_ ++ _)

  def avoidEmpty: NonEmpty = {
    val result = if (self.isEmpty) {
      TraceSet(Set(Trace.NoOp))
    } else {
      this
    }
    result.asInstanceOf[NonEmpty]
  }

}
