package org.apache.spark.ml.dsl.utils

import java.util.Date

import org.apache.spark.ml.dsl.AbstractFlowSuite
import org.json4s.MappingException

/**
  * Created by peng on 06/05/16.
  */
case class TimeWrapper(time: Date)

case class Users(user: Seq[User]) {
}

case class User(
                 name: String,
                 roles: Option[Roles] = None
               )

case class Roles(role: Seq[String]) {
}

class MessageRelaySuite extends AbstractFlowSuite {

  test("SerializingParam[Function1] should work") {
    val fn = {k: Int => 2*k}
    val reader = new MessageReader[Int => Int]
    val param = reader.Param("id", "name", "")

    val json = param.jsonEncode(fn)
    println(json)
    val fn2 = param.jsonDecode(json)
    assert(fn2(2) == 4)
  }

  test("can convert generated timestamp") {
    val date = new Date()
    val obj = TimeWrapper(new Date())

    val xmlStr = MessageView(obj).toXMLStr(pretty = false)
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe(s"TimeWrapper($date)")
  }

  test("can convert lossless timestamp") {
    val str = "2016-09-08T15:00:00.0Z"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("TimeWrapper(Thu Sep 08 15:00:00 EDT 2016)")
  }

  test("can convert less accurate timestamp") {
    val str = "2016-09-08T15:00:00Z"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("TimeWrapper(Thu Sep 08 15:00:00 EDT 2016)")
  }

  test("can convert even less accurate timestamp") {
    val str = "2016-09-08T15:00"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("TimeWrapper(Thu Sep 08 15:00:00 EDT 2016)")
  }

  test("reading an object with missing value & default value should fail early") {

    val xmlStr = s"<node><roles><role>DC</role></roles></node>"

    val reader = new MessageReader[User]()
    intercept[MappingException] {
      val v = reader.fromXML(xmlStr)
    }
  }

  test("reading an array with misplaced value & default value should fail early") {

    val xmlStr =
      s"""
         |<users>
         |<user>user1@domain.com<roles><role>DC</role></roles></user>
         |<user>user2@domain.com<roles><role>DC</role></roles></user>
         |</users>
       """.stripMargin

    val reader = new MessageReader[Users]()
    intercept[MappingException] {
      val v = reader.fromXML(xmlStr)
    }
  }

  test("reading an array with missing value & default value should fail early") {

    val xmlStr =
      s"""
         |<users>
         |<user><roles><role>DC</role></roles></user>
         |<user><roles><role>DC</role></roles></user>
         |</users>
       """.stripMargin

    val reader = new MessageReader[Users]()
    intercept[MappingException] {
      val v = reader.fromXML(xmlStr)
    }
  }

  test("reading an object from a converted string should work") {

    val xmlStr = MessageView(User(name = "30")).toXMLStr(pretty = false)
    print(xmlStr)

    val reader = new MessageReader[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(30,None)")
  }

  test("reading an object with provided value should work") {

    val xmlStr = s"<node><name>string</name><roles><role>DC</role></roles></node>"

    val reader = new MessageReader[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(string,Some(Roles(List(DC))))")
  }

  test("reading an object with default value should work") {

    val xmlStr = s"<node><name>string</name></node>"

    val reader = new MessageReader[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(string,None)")
  }
}
