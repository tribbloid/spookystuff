package org.apache.spark.ml.dsl.utils.messaging

import java.util.Date

import org.apache.spark.ml.dsl.AbstractFlowSuite
import org.json4s.MappingException

case class TimeWrapper(time: Date)

case class UsersWrapper(a: String, users: Users)

case class Users(user: Seq[User])

object user

case class User(
                 name: String,
                 roles: Option[Roles] = None
               )

case class Roles(role: Seq[String])

case class Multipart(a: String, b: String)(c: Int = 10)

object Multipart extends MessageReader[Multipart] {
}

//case object ObjectExample1 extends AbstractObjectExample

class MessageReaderSuite extends AbstractFlowSuite {

  //TODO: disabled before FallbackSerializer is really put to use
  ignore("SerializingParam[Function1] should work") {
    val fn = {k: Int => 2*k}
    val reader = new MessageReader[Int => Int]()
    val param = reader.Param("id", "name", "")

    val json = param.jsonEncode(fn)
    println(json)
    val fn2 = param.jsonDecode(json)
    assert(fn2(2) == 4)
  }

  val date = new Date()

  it("can read generated timestamp") {
    val obj = TimeWrapper(date)

    val xmlStr = MessageWriter(obj).toXMLStr(pretty = false)
    xmlStr.shouldBeLike(
      s"<TimeWrapper><time>......</time></TimeWrapper>"
    )

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe(s"TimeWrapper($date)")
  }

  it("can read lossless timestamp") {
    val str = "2016-09-08T15:00:00.0Z"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBeLike("TimeWrapper(Thu Sep 08 15:00:00......)")
  }

  it("can read less accurate timestamp") {
    val str = "2016-09-08T15:00:00Z"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBeLike("TimeWrapper(Thu Sep 08 15:00:00......)")
  }

  it("can convert even less accurate timestamp") {
    val str = "2016-09-08T15:00"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBeLike("TimeWrapper(Thu Sep 08 15:00:00......)")
  }

  it("reading an object with missing value & default value should fail early") {

    val xmlStr = s"<node><roles><role>DC</role></roles></node>"

    val reader = new MessageReader[User]()
    intercept[MappingException] {
      reader.fromXML(xmlStr)
    }
  }

  it("reading an array with misplaced value & default value should fail early") {

    val xmlStr =
      s"""
         |<users>
         |<user>user1@domain.com<roles><role>DC</role></roles></user>
         |<user>user2@domain.com<roles><role>DC</role></roles></user>
         |</users>
       """.stripMargin

    val reader = new MessageReader[Users]()
    intercept[MappingException] {
      reader.fromXML(xmlStr)
    }
  }

  it("reading a wrapped array with misplaced value & default value should fail early") {

    val xmlStr =
      s"""
         |<node>
         |<a>name</a>
         |<users>
         |<user>user1@domain.com<roles><role>DC</role></roles></user>
         |<user>user2@domain.com<roles><role>DC</role></roles></user>
         |</users>
         |</node>
       """.stripMargin

    val reader = new MessageReader[UsersWrapper]()
    intercept[MappingException] {
      reader.fromXML(xmlStr)
    }
  }

  it("reading an array with missing value & default value should fail early") {

    val xmlStr =
      s"""
         |<users>
         |<user><roles><role>DC</role></roles></user>
         |<user><roles><role>DC</role></roles></user>
         |</users>
       """.stripMargin

    val reader = new MessageReader[Users]()
    intercept[MappingException] {
      reader.fromXML(xmlStr)
    }
  }

  it("reading an object from a converted string should work") {

    val xmlStr = MessageWriter(User(name = "30")).toXMLStr(pretty = false)
    xmlStr.shouldBe(
      "<User><name>30</name></User>"
    )

    val reader = new MessageReader[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(30,None)")
  }

  it("reading an object with provided value should work") {

    val xmlStr = s"<node><name>string</name><roles><role>DC</role></roles></node>"

    val reader = new MessageReader[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(string,Some(Roles(List(DC))))")
  }

  it("reading an object with default value should work") {

    val xmlStr = s"<node><name>string</name></node>"

    val reader = new MessageReader[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(string,None)")
  }

  it("Multipart case class JSON read should be broken") {
    val ex = Multipart("aa", "bb")(3)
    val jsonStr = MessageWriter(ex).toJSON()
    jsonStr.shouldBe(
      s"""
         |{
         |  "a" : "aa",
         |  "b" : "bb"
         |}
      """.stripMargin
    )

    val reader = implicitly[MessageReader[Multipart]]

    intercept[MappingException] {
      MessageReader._fromJSON[Multipart](jsonStr)(reader)
    }
  }
}
