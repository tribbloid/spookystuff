package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Agent

/**
  * Created by peng on 1/21/15.
  */
@SerialVersionUID(-3444865880420843541L)
abstract class Assertion extends Action {

  final override def skeleton: None.type = None // can be omitted

  final override def doExe(agent: Agent): Seq[Doc] = {

    exeNoOutput(agent: Agent)

    Nil
  }

  def exeNoOutput(agent: Agent): Unit
}
