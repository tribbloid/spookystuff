package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions._
import org.apache.spark.ml.dsl.utils.RecursiveMessageRelay

/**
  * Created by peng on 15/06/16.
  */
class ExtractorsSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.dsl._

  it("Literal -> JSON") {
    val lit: Lit[FR, Int] = Lit(1)

    val json = lit.toJSON()
    json.shouldBe (
      "1"
    )
  }

  it("Action with Literal -> JSON") {
    val action = Wget(Lit("http://dummy.org"))

    val json = RecursiveMessageRelay.toMessage(action).toJSON()
    json.shouldBe(
      """
        |{
        |  "uri" : "http://dummy.org",
        |  "filter" : { }
        |}
      """.stripMargin
    )
  }

  it("Literal.toString") {
    val str = Lit("lit").toString
    assert(str == "lit")
  }


  it("Click -> JSON") {
    val action = Click("o1")
    val json = RecursiveMessageRelay.toMessage(action).toJSON()
    json.shouldBe(
      """
        |{
        |  "selector" : "o1",
        |  "delay" : "0 seconds",
        |  "blocking" : true
        |}
      """.stripMargin
    )
  }

  it("Wget -> JSON") {
    val action = Wget("http://dummy.com")
    val json = RecursiveMessageRelay.toMessage(action).toJSON()
    json.shouldBe(
      """
        |{
        |  "uri" : "http://dummy.com",
        |  "filter" : { }
        |}
      """.stripMargin
    )
  }

  it("Loop -> JSON") {
    val action = Loop(
      Click("o1")
        +> Snapshot()
    )
    val json = RecursiveMessageRelay.toMessage(action).toJSON()
    json.shouldBe(
      """
        |{
        |  "children" : {
        |    "hd$1" : {
        |      "selector" : "o1",
        |      "delay" : "0 seconds",
        |      "blocking" : true
        |    },
        |    "tl$1" : {
        |      "hd$1" : {
        |        "filter" : { }
        |      },
        |      "tl$1" : { }
        |    }
        |  },
        |  "limit" : 2147483647
        |}
      """.stripMargin
    )
  }
}
