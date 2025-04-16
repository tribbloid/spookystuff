package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.HasTrace.NoStateChange
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Observation

trait Foundation extends Serializable {

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

  case object NoOp extends HasTrace with NoStateChange {
    override def trace: Trace = Trace(Nil)

    override def apply(agent: Agent): Seq[Observation] = Nil
  }
}
