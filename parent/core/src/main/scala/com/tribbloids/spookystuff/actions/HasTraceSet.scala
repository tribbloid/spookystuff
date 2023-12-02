package com.tribbloids.spookystuff.actions

import scala.reflect.ClassTag

object HasTraceSet {}

trait HasTraceSet {

  def asTraceSet: TraceSet

  def *>[T: ClassTag](others: IterableOnce[T]): TraceSet = {
    val result = asTraceSet.flatMap(trace =>
      others.map {
        case otherAction: Action => trace :+ otherAction
        case otherList: List[_] =>
          trace ++ otherList.collect {
            case v: Action => v
          }
      }
    )
    TraceSet(result.map(v => v: Trace))
  }

  def ||(other: IterableOnce[Trace]): TraceSet = TraceSet(asTraceSet ++ other)
}
