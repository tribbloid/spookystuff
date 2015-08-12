package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.session.Session

/**
 * Created by peng on 1/21/15.
 */
abstract class Assertion extends Action {

  final override def outputNames = Set()

  final override def trunk = None //can't be omitted

  final override def doExe(session: Session): Seq[Page] = {

    exeWithoutPage(session: Session)

    Seq()
  }

  def exeWithoutPage(session: Session): Unit
}

case class Assert(condition: Page => Boolean) extends Assertion {
  override def exeWithoutPage(session: Session): Unit = {
    val page = DefaultSnapshot.apply(session).head.asInstanceOf[Page]

    assert(condition(page))
  }
}