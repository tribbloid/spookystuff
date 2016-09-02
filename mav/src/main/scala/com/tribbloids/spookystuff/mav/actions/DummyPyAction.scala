package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.{DocFilter, Export, PyAction}
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Fetched}
import com.tribbloids.spookystuff.dsl.DocFilters
import com.tribbloids.spookystuff.session.Session
import org.apache.http.entity.ContentType

/**
  * Created by peng on 30/08/16.
  */
@SerialVersionUID(-6784287573066896999L)
case class DummyPyAction(
                          a: Int = 1
                        ) extends Export with PyAction {

  override def doExeNoName(session: Session): Seq[Fetched] = {
    val result = Py(session).exe(
      Map("b" -> 2, "c" -> 3)
    )
    val doc = new Doc(
      DocUID(List(this), this),
      "dummy",
      Some(ContentType.TEXT_PLAIN.getMimeType),
      result.mkString("\n").getBytes("UTF-8")
    )
    Seq(doc)
  }
}
