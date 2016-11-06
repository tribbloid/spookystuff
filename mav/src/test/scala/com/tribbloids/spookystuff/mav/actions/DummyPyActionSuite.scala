package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.doc.Doc

/**
  * Created by peng on 01/09/16.
  */
class DummyPyActionSuite extends SpookyEnvFixture {

  val action = DummyPyAction()

  test("can execute on driver") {

    val doc = action.fetch(spooky)
    doc.flatMap(_.asInstanceOf[Doc].code).mkString("\n").shouldBe("6")

    //assuming that lazy interpret is effective
    assert(spooky.metrics.pythonInterpretationSuccess.value == 3)

//    val processes = JProcesses.getProcessList()
//      .asScala
//    val pythonProcesses = processes.filter(_.getName == "python")
//    assert(pythonProcesses.size == 1)
  }

  import com.tribbloids.spookystuff.dsl._

  test("can execute on workers") {
    val df = sql
      .createDataFrame((0 to 32)
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
        |{"_1":17,"_c1":"102"}
        |{"_1":18,"_c1":"108"}
        |{"_1":19,"_c1":"114"}
        |{"_1":2,"_c1":"12"}
        |{"_1":20,"_c1":"120"}
        |{"_1":21,"_c1":"126"}
        |{"_1":22,"_c1":"132"}
        |{"_1":23,"_c1":"138"}
        |{"_1":24,"_c1":"144"}
        |{"_1":25,"_c1":"150"}
        |{"_1":26,"_c1":"156"}
        |{"_1":27,"_c1":"162"}
        |{"_1":28,"_c1":"168"}
        |{"_1":29,"_c1":"174"}
        |{"_1":3,"_c1":"18"}
        |{"_1":30,"_c1":"180"}
        |{"_1":31,"_c1":"186"}
        |{"_1":32,"_c1":"192"}
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