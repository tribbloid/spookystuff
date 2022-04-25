package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.row.SpookySchema

import scala.language.implicitConversions

object TraceSetView {}

//The precedence of an inﬁx operator is determined by the operator’s ﬁrst character.
//Characters are listed below in increasing order of precedence, with characters on
//the same line having the same precedence.
//(all letters)
//|
//^
//&
//= !.................................................(new doc)
//< >
//= !.................................................(old doc)
//:
//+ -
//* / %
//(all other special characters)
//now using immutable pattern to increase maintainability
//put all narrow transformation closures here
case class TraceSetView(asTraceSet: Set[Trace]) extends TraceSetAPI {

  //many-to-one
  def +>(another: Action): TraceSetView = TraceSetView(asTraceSet.map(trace => trace :+ another))
  def +>(others: Trace): TraceSetView = TraceSetView(asTraceSet.map(trace => trace ++ others))

  def outputNames: Set[String] = asTraceViewSet.map(_.outputNames).reduce(_ ++ _)

  def rewriteGlobally(schema: SpookySchema): TraceSetView = {
    val result = asTraceViewSet.flatMap { v =>
      v.rewriteGlobally(schema)
    }
    TraceSetView(result.map(_.asTrace))
  }
}
