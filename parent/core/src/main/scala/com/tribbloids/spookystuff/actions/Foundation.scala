package com.tribbloids.spookystuff.actions

import scala.language.implicitConversions

object Foundation {

  trait HasTraceSet {

    def traceSet: Set[Trace]

    def *>(that: HasTraceSet): TraceSet = {
      val newTraces = this.traceSet.flatMap(left =>
        that.traceSet.map { right =>
          left +> right
        }
      )
      TraceSet(newTraces.map(v => v: Trace))
    }

    def ||(other: HasTraceSet): TraceSet = TraceSet(traceSet ++ other.traceSet)
  }

  /**
    * read [[ai.acyclic.prover.commons.__OperatorPrecedence]] when defining new operators
    * @param traceSet
    *   no duplicated Trace
    */
  case class TraceSet(traceSet: Set[Trace]) extends HasTraceSet {

    def outputNames: Set[String] = traceSet.map(_.outputNames).reduce(_ ++ _)

    //      def avoidEmpty: NonEmpty = {
    //        val result = if (traces.isEmpty) {
    //          FetchImpl(Set(Trace.NoOp))
    //        } else {
    //          this
    //        }
    //        result.asInstanceOf[NonEmpty]
    //      }
  }

  object TraceSet {

    def of(traces: Trace*): TraceSet = TraceSet(traces.toSet)
  }

//  type Induction[+T] = Induction.K[T]
  object Induction {

//    sealed trait K[+T] {
//
////      final def withRow[T2](row: T2): FetchAndUpdateRows[T2] = {
////        FetchAndUpdateRowsImpl(traces.map(v => (v, row)).toSeq)
////      }
//
////      final def ->[T2](row: T2): OutboundWithRowUpdate[T2] = withRow(row)
//    }
//
//    case object Terminate extends Induction[Nothing] {
//      // delete row, won't appear in result or participate in further computation
//      // by default, NoOp in explore will converted to terminate automatically
//
//      override def traces: Set[Trace] = Set.empty
//    }

//    trait NonEmptyCap { self: Fetch => }
//    type NonEmpty = Fetch & NonEmptyCap
  }

  trait HasTrace extends HasTraceSet {

    def trace: Trace

    @transient final override lazy val traceSet: Set[Trace] = Set(trace)

    // many-to-one
    //  def +>(another: Action): Trace = Trace(asTrace :+ another)
    def +>(that: HasTrace): Trace = {

//      (this, that) match {
//        case (NoOp, _) => NoOp
//        case (_, NoOp) => NoOp // TODO: should this be changed to EndOfStream?
//        case _         => Trace(asTrace ++ that.asTrace)
//      }

      Trace(trace ++ that.trace)
    }
  }

  object HasTrace {

    implicit def unbox(v: HasTrace): Trace = v.trace
  }

}
