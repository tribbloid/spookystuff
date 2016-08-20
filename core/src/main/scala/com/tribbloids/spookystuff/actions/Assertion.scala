package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Session

/**
 * Created by peng on 1/21/15.
 */
@SerialVersionUID(-3444865880420843541L)
abstract class Assertion extends Action {

  final override def outputNames = Set()

  final override def trunk = None //can't be omitted

  final override def doExe(session: Session): Seq[Doc] = {

    exeWithoutPage(session: Session)

    Nil
  }

  def exeWithoutPage(session: Session): Unit
}

@SerialVersionUID(-5210711420423079523L)
case class Assert(condition: DocCondition) extends Assertion {
  override def exeWithoutPage(session: Session): Unit = {
    val page = DefaultSnapshot.apply(session).head.asInstanceOf[Doc]

    assert(condition(page, session))
  }
}