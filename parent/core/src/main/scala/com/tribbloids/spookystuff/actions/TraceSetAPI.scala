package com.tribbloids.spookystuff.actions

import scala.reflect.ClassTag

import scala.language.implicitConversions

object TraceSetAPI {

  implicit def unbox(v: TraceSetAPI): Set[Trace] = v.asTraceSet
  implicit def unboxView(v: TraceSetAPI): TraceSetView = v.traceSetView
}

trait TraceSetAPI {

  def asTraceSet: Set[Trace]
  @transient final lazy val traceSetView: TraceSetView = TraceSetView(asTraceSet)
  @transient lazy val asTraceViewSet: Set[TraceView] = asTraceSet.map(v => TraceView(v))

  def *>[T: ClassTag](others: TraversableOnce[T]): TraceSetView = {
    val result = asTraceSet.flatMap(trace =>
      others.map {
        case otherAction: Action => trace :+ otherAction
        case otherList: List[_] =>
          trace ++ otherList.collect {
            case v: Action => v
          }
      }
    )
    TraceSetView(result)
  }

  def ||(other: TraversableOnce[Trace]): TraceSetView = TraceSetView(asTraceSet ++ other)
}
