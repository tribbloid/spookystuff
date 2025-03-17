package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Agent

/**
  * Created by peng on 1/21/15.
  */
@SerialVersionUID(-3444865880420843541L)
abstract class AssertionLike extends Action {

  final override def stateChange: None.type = None // can be omitted

  final override def doExe(agent: Agent): Seq[Doc] = {

    exeNoOutput(agent: Agent)

    Nil
  }

  def exeNoOutput(agent: Agent): Unit
}
