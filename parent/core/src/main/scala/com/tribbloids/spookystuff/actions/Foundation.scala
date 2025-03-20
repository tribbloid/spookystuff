package com.tribbloids.spookystuff.actions


object Foundation {

//  type TraceSet = Set[Trace]

  trait HasTraceSet {

    def traceSet: Set[Trace]

    def *>(that: HasTraceSet): Set[Trace] = {
      val newTraces = this.traceSet.flatMap(left =>
        that.traceSet.map { right =>
          left +> right
        }
      )
      newTraces.map(v => v: Trace)
    }

    def ||(other: HasTraceSet): Set[Trace] = traceSet ++ other.traceSet
  }

//  object TraceSet {
//
//    def of(traces: Trace*): TraceSet = TraceSet(traces.toSet)
//  }

//  type Induction[+T] = Induction.K[T]
//  object Induction {
//
////    sealed trait K[+T] {
////
//////      final def withRow[T2](row: T2): FetchAndUpdateRows[T2] = {
//////        FetchAndUpdateRowsImpl(traces.map(v => (v, row)).toSeq)
//////      }
////
//////      final def ->[T2](row: T2): OutboundWithRowUpdate[T2] = withRow(row)
////    }
////
////    case object Terminate extends Induction[Nothing] {
////      // delete row, won't appear in result or participate in further computation
////      // by default, NoOp in explore will converted to terminate automatically
////
////      override def traces: Set[Trace] = Set.empty
////    }
//
////    trait NonEmptyCap { self: Fetch => }
////    type NonEmpty = Fetch & NonEmptyCap
//  }

//  trait HasTrace extends HasTraceSet {
//
//    def trace: Trace
//
//  }
}
