package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.Export
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Fetched}
import com.tribbloids.spookystuff.extractors.{Extractor, Literal}
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.session.python.CaseInstanceRef
import org.apache.http.entity.ContentType

/**
  * Created by peng on 30/08/16.
  */
@SerialVersionUID(-6784287573066896999L)
case class DummyPyAction(
                          a: Extractor[Int] = Literal(1)
                        ) extends Export with CaseInstanceRef {

  override def doExeNoName(session: Session): Seq[Fetched] = {
    val repr1 = Py(session).dummy(2, 3).$repr
    val repr2 = Py(session).dummy(b = 3, c = 2).$repr
    assert(repr1 == repr2)
    val doc = new Doc(
      uid = DocUID(List(this), this)(),
      uri = "dummy",
      declaredContentType = Some(ContentType.TEXT_PLAIN.toString),
      raw = repr1.get.getBytes("UTF-8")
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

class PythonActionSuite extends SpookyEnvFixture {

  val action = DummyPyAction()

  test("can be created on python") {
    action.createOpt.get.shouldBe(

    )
  }

  test("can execute on driver") {

    val doc = action.fetch(spooky)
    doc.flatMap(_.asInstanceOf[Doc].code).mkString("\n").shouldBe("6")

    //assuming that lazy interpret is effective
    assert(spooky.metrics.pythonInterpretationSuccess.value <= 3)

//    val processes = JProcesses.getProcessList()
//      .asScala
//    val pythonProcesses = processes.filter(_.getName == "python")
//    assert(pythonProcesses.size == 1)
  }

  import com.tribbloids.spookystuff.dsl._

  test("can execute on workers") {
    val df = sql
      .createDataFrame((0 to 16)
        .map(v => Tuple1(v)))
    val ds = spooky.create(
      df
    )
    val result = ds
      .fetch(
        DummyPyAction('_1.typed[Int]) ~ 'A
      )
      .extract(S.code)
      .toJSON()
      .collect()
      .sortBy(identity)
      .mkString("\n")

    result.shouldBe(
      """
        |{"_1":0,"_c1":"0"}
        |{"_1":1,"_c1":"6"}
        |{"_1":10,"_c1":"60"}
        |{"_1":11,"_c1":"66"}
        |{"_1":12,"_c1":"72"}
        |{"_1":13,"_c1":"78"}
        |{"_1":14,"_c1":"84"}
        |{"_1":15,"_c1":"90"}
        |{"_1":16,"_c1":"96"}
        |{"_1":2,"_c1":"12"}
        |{"_1":3,"_c1":"18"}
        |{"_1":4,"_c1":"24"}
        |{"_1":5,"_c1":"30"}
        |{"_1":6,"_c1":"36"}
        |{"_1":7,"_c1":"42"}
        |{"_1":8,"_c1":"48"}
        |{"_1":9,"_c1":"54"}
      """.stripMargin
    )
  }
}