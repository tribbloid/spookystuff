package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.Impl
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Doc

/**
  * Created by peng on 3/26/15.
  */
package object actions {

  type DocFilter = Impl.Fn[(Doc, Agent), Doc] // TODO: merge with Selector[Doc]

  type DocCondition = Impl.Fn[(Doc, Agent), Boolean] // TODO: merge with Selector[Doc]
}
