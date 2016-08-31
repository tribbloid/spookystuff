package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.actions.{DocFilter, Export, PyAction}
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Fetched}
import com.tribbloids.spookystuff.dsl.DocFilters
import com.tribbloids.spookystuff.extractors.Extractor
import com.tribbloids.spookystuff.session.Session

/**
  * Created by peng on 30/08/16.
  */
@SerialVersionUID(-6784287573066896999L)
case class DummyPyAction(
                              a: Int = 1
                            ) extends PyAction with Export {

  override protected def doExeNoName(session: Session): Seq[Fetched] = {
    val result = Py(session).exe(
      Map("b" -> 2, "c" -> 3)
    )
    val doc = new Doc(
      DocUID(List(this), this),
      "dummy",
      Some("utf-8"),
      result.mkString("\n").getBytes("UTF-8")
    )
    Seq(doc)
  }

  override def filter: DocFilter = DocFilters.AllowStatusCode2XX
}
