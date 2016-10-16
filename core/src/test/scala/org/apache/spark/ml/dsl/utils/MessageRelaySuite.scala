package org.apache.spark.ml.dsl.utils

import java.util.Date

import org.apache.spark.ml.dsl.AbstractFlowSuite

/**
  * Created by peng on 06/05/16.
  */
case class TimeWrapper(time: Date)

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
    val date = TimeWrapper(new Date())

    val xmlStr = MessageView(date).toXMLStr(pretty = false)
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
  }

  test("can convert lossless timestamp") {
    val str = "2016-09-08T15:00:00.0Z"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    println(v)
  }

  test("can convert less accurate timestamp") {
    val str = "2016-09-08T15:00:00Z"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    println(v)
  }

  test("can convert even less accurate timestamp") {
    val str = "2016-09-08T15:00"
    val xmlStr = s"<TimeWrapper><time>$str</time></TimeWrapper>"
    println(xmlStr)

    val reader = new MessageReader[TimeWrapper]()
    val v = reader.fromXML(xmlStr)
    println(v)
  }
}
