package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.actions.HasTrace.NoStateChange
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.{Doc, Observation}

/**
  * Created by peng on 3/26/15.
  */
package object actions {

  type DocFilter = Hom.Impl.Circuit[(Doc, Agent), Doc]

  type DocCondition = Hom.Impl.Circuit[(Doc, Agent), Boolean]

  case object NoOp extends HasTrace with NoStateChange {
    override def trace: Trace = Trace(Nil)

    override def apply(agent: Agent): Seq[Observation] = Nil
  }

//  case object Discard extends HasTraceSet {
//    override def traceSet: Set[Trace] = Set.empty
//  }
}
