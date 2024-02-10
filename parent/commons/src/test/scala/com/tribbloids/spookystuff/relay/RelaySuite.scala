package com.tribbloids.spookystuff.relay

import com.tribbloids.spookystuff.relay.TestBeans._
import com.tribbloids.spookystuff.relay.io.Encoder
import com.tribbloids.spookystuff.testutils.BaseSpec
import org.json4s.MappingException
import org.json4s.reflect.{Executable, ParanamerReader}

import java.util.Date

class RelaySuite extends BaseSpec {

  val date: Date = new Date()

  it("Paranamer constructor lookup") {

    val clazz = classOf[TimeWrapper]
    val tors = clazz.getConstructors
    val ctor = tors.head
    val exe = new Executable(ctor, true)

    val names = ParanamerReader.lookupParameterNames(exe)

    require(names.size == 1)
  }

  it("can read generated timestamp") {
    val obj = TimeWrapper(date)

    val xmlStr = Encoder.forValue(obj).toXMLStr(pretty = false)
    xmlStr.shouldBeLike(
      s"<TimeWrapper><time>......</time></TimeWrapper>"
    )

    val reader = new Relay.ToSelf[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe(s"TimeWrapper($date)")
  }

  it("can read lossless timestamp") {
    val str = "2016-09-08T15:00:00.0Z"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"

    val reader = new Relay.ToSelf[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBeLike("TimeWrapper(Thu Sep 08 15:00:00......)")
  }

  it("can read less accurate timestamp") {
    val str = "2016-09-08T15:00:00Z"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"

    val reader = new Relay.ToSelf[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBeLike("TimeWrapper(Thu Sep 08 15:00:00......)")
  }

  it("can convert even less accurate timestamp") {
    val str = "2016-09-08T15:00"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"

    val reader = new Relay.ToSelf[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBeLike("TimeWrapper(Thu Sep 08 15:00:00......)")
  }

  it("reading an object with missing value & default value should fail early") {

    val xmlStr = s"<node><roles><role>DC</role></roles></node>"

    val reader = new Relay.ToSelf[User]()
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

    val reader = new Relay.ToSelf[Users]()
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

    val reader = new Relay.ToSelf[UsersWrapper]()
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

    val reader = new Relay.ToSelf[Users]()
    intercept[MappingException] {
      reader.fromXML(xmlStr)
    }
  }

  it("reading an object from a converted string should work") {

    val xmlStr = Encoder.forValue(User(name = "30")).toXMLStr(pretty = false)
    xmlStr.shouldBe(
      "<User><name>30</name></User>"
    )

    val reader = new Relay.ToSelf[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(30,None)")
  }

  it("reading an object with provided value should work") {

    val xmlStr = s"<node><name>string</name><roles><role>DC</role></roles></node>"

    val reader = new Relay.ToSelf[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(string,Some(Roles(List(DC))))")
  }

  it("reading an object with default value should work") {

    val xmlStr = s"<node><name>string</name></node>"

    val reader = new Relay.ToSelf[User]()
    val v = reader.fromXML(xmlStr)
    v.toString.shouldBe("User(string,None)")
  }

  it("Multipart case class JSON read should be broken") {
    val ex = Multipart("aa", "bb")(3)
    val jsonStr = Encoder.forValue(ex).toJSON()
    jsonStr.shouldBe(
      s"""
         |{
         |  "a" : "aa",
         |  "b" : "bb"
         |}
      """.stripMargin
    )

    val reader = implicitly[Relay.ToSelf[Multipart]]

    intercept[MappingException] {
      reader.fromJSON(jsonStr)
    }
  }
}
