package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.AbstractSession

/**
 * Created by peng on 1/21/15.
 */
@SerialVersionUID(-3444865880420843541L)
abstract class Assertion extends Action {

  final override def outputNames = Set()

  final override def trunk = None //can be omitted

  final override def doExe(session: AbstractSession): Seq[Doc] = {

    exeNoOutput(session: AbstractSession)

    Nil
  }

  def exeNoOutput(session: AbstractSession): Unit
}

@SerialVersionUID(-5210711420423079523L)
case class Assert(condition: DocCondition) extends Assertion {
  override def exeNoOutput(session: AbstractSession): Unit = {
    val page = QuickSnapshot.apply(session).head.asInstanceOf[Doc]

    assert(condition(page, session))
  }
}