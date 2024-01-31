package com.tribbloids.spookystuff.web

import ai.acyclic.prover.commons.function.PreDef.Fn
import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Agent

/**
  * Created by peng on 3/26/15.
  */
package object actions {

//  type GenTrace = Seq[Action]

  type Trace = List[Action]

  type DryRun = List[Trace]

  type DocFilter = Fn[(Doc, Agent), Doc] // TODO: merge with Selector[Doc]

  type DocCondition = Fn[(Doc, Agent), Boolean] // TODO: merge with Selector[Doc]
}
