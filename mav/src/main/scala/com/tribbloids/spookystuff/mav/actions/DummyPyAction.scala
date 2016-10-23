package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.{Export, PyAction}
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Fetched}
import com.tribbloids.spookystuff.extractors.{Extractor, Literal}
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import org.apache.http.entity.ContentType

/**
  * Created by peng on 30/08/16.
  */
@SerialVersionUID(-6784287573066896999L)
case class DummyPyAction(
                          a: Extractor[Int] = Literal(1)
                        ) extends Export with PyAction {

  override def doExeNoName(session: Session): Seq[Fetched] = {
    val result = Py(session).dummy(2, 3)
    val result2 = Py(session).dummy(b = 3, c = 2)
    assert(result == result2)
    val doc = new Doc(
      uid = DocUID(List(this), this),
      uri = "dummy",
      declaredContentType = Some(ContentType.TEXT_PLAIN.toString),
      content = result.getBytes("UTF-8")
    )
    Seq(doc)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema) = {

    a.resolve(schema)
      .lift
      .apply(pageRow)
      .map(
        v =>
          this.copy(a = Literal.erase(v)).asInstanceOf[this.type]
      )
  }
}