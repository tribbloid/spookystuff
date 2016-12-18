package com.tribbloids.spookystuff.session.python

import com.tribbloids.spookystuff.testutils.TestMixin
import org.apache.spark.ml.dsl.utils.Message
import org.scalatest.FunSuite

/**
  * Created by peng on 27/11/16.
  */

case class JSONExample(
                        a: Int,
                        var bOpt: Option[String]
                      ) extends JSONInstanceRef with Message

case class CaseExample(
                        a: Int,
                        var bOpt: Option[String]
//                        child1: Option[CaseExample] = None,
//                        child2: Option[JSONExample] = None
                      ) extends CaseInstanceRef

class PyRefSuite extends FunSuite with TestMixin {

  test("JSONInstanceRef can initialize Python instance with missing constructor parameter") {
    val example = JSONExample(1, None)
    val str = example.createOpt.get
    str.shouldBe(
      s"""
         |pyspookystuff.session.python.JSONExample(**(json.loads(
         |${PyConverter.QQQ}
         |{
         |  "a" : 1
         |}
         |${PyConverter.QQQ}
         |)))
      """.stripMargin
    )
  }

  test("JSONInstanceRef can initialize Python instance after constructor parameter has been changed") {
    val example = JSONExample(1, Some("a"))
    val str = example.createOpt.get
    str.shouldBe(
      s"""
         |pyspookystuff.session.python.JSONExample(**(json.loads(
         |${PyConverter.QQQ}
         |{
         |  "a" : 1,
         |  "bOpt" : "a"
         |}
         |${PyConverter.QQQ}
         |)))
      """.stripMargin
    )
    example.bOpt = None
    val str2 = example.createOpt.get
    str2.shouldBe(
      s"""
         |pyspookystuff.session.python.JSONExample(**(json.loads(
         |${PyConverter.QQQ}
         |{
         |  "a" : 1
         |}
         |${PyConverter.QQQ}
         |)))
      """.stripMargin
    )
  }

  test("CaseExample can initialize Python instance with missing constructor parameter") {
    val example = CaseExample(1, Some("a"))
    example.bOpt = None
    val str = example.createOpt.get
    str.shouldBe(
      s"""
         |pyspookystuff.session.python.CaseExample(a=json.loads(
         |${PyConverter.QQQ}
         |1
         |${PyConverter.QQQ}
         |))
      """.stripMargin
    )
  }

  test("CaseExample can initialize Python instance after constructor parameter has been changed") {
    val example = CaseExample(1, Some("a"))
    val str = example.createOpt.get
    str.shouldBe(
      s"""
         |pyspookystuff.session.python.CaseExample(a=json.loads(
         |${PyConverter.QQQ}
         |1
         |${PyConverter.QQQ}
         |), bOpt=json.loads(
         |${PyConverter.QQQ}
         |"a"
         |${PyConverter.QQQ}
         |))
      """.stripMargin
    )
    example.bOpt = None
    val str2 = example.createOpt.get
    str2.shouldBe(
      s"""
         |pyspookystuff.session.python.CaseExample(a=json.loads(
         |${PyConverter.QQQ}
         |1
         |${PyConverter.QQQ}
         |))
      """.stripMargin
    )
  }
}
