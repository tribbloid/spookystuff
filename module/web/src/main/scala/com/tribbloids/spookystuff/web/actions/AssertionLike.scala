package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Harness

/**
  * Created by peng on 1/21/15.
  */
@SerialVersionUID(-3444865880420843541L)
abstract class AssertionLike extends Action {

  override val isStateful: Boolean = false

  final override def doExe(agent: Harness): Seq[Doc] = {

    exeNoOutput(agent: Harness)

    Nil
  }

  def exeNoOutput(agent: Harness): Unit
}
