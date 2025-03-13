package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.DocCondition
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Agent

@SerialVersionUID(-5210711420423079523L)
// TODO: remove, no need if manipulation agent directly
case class Assert(condition: DocCondition) extends AssertionLike {
  override def exeNoOutput(agent: Agent): Unit = {
    val page = Snapshot.QuickSnapshot.apply(agent).head.asInstanceOf[Doc]

    assert(condition(page -> agent))
  }
}
