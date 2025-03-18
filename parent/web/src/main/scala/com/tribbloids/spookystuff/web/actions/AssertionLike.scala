package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.actions.HasTrace.NoStateChange
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Agent

/**
  * Created by peng on 1/21/15.
  */
@SerialVersionUID(-3444865880420843541L)
abstract class AssertionLike extends Action with NoStateChange {

  final override def doExe(agent: Agent): Seq[Doc] = {

    exeNoOutput(agent: Agent)

    Nil
  }

  def exeNoOutput(agent: Agent): Unit
}
