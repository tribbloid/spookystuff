package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Session

@SerialVersionUID(-5210711420423079523L)
case class Assert(condition: DocCondition) extends AssertionLike {
  override def exeNoOutput(session: Session): Unit = {
    val page = Snapshot.QuickSnapshot.apply(session).head.asInstanceOf[Doc]

    assert(condition(page -> session))
  }
}
