package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.actions.Foundation.HasTrace
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Doc

/**
  * Created by peng on 3/26/15.
  */
package object actions {

  type DocFilter = Hom.Impl.Circuit[(Doc, Agent), Doc] // TODO: merge with Selector[Doc]

  type DocCondition = Hom.Impl.Circuit[(Doc, Agent), Boolean] // TODO: merge with Selector[Doc]

  case object NoOp extends HasTrace {
    override def trace: Trace = Trace(Nil)
  }

//  case object Discard extends HasTraceSet {
//    override def traceSet: Set[Trace] = Set.empty
//  }
}
