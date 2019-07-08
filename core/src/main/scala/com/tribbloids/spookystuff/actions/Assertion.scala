package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Session

/**
  * Created by peng on 1/21/15.
  */
@SerialVersionUID(-3444865880420843541L)
abstract class Assertion extends Action {

  final override def skeleton = None //can be omitted

  final override def doExe(session: Session): Seq[Doc] = {

    exeNoOutput(session: Session)

    Nil
  }

  def exeNoOutput(session: Session): Unit
}

@SerialVersionUID(-5210711420423079523L)
case class Assert(condition: DocCondition) extends Assertion {
  override def exeNoOutput(session: Session): Unit = {
    val page = QuickSnapshot.apply(session).head.asInstanceOf[Doc]

    assert(condition(page -> session))
  }
}
