package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.extractors.impl.Lit
import org.apache.spark.ml.dsl.utils.messaging.RecursiveMessageRelay

/**
  * Created by peng on 15/06/16.
  */
class ExtractorsSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.dsl._

  it("Literal -> JSON") {
    val lit: Lit[FR, Int] = Lit(1)

    val json = lit.prettyJSON()
    json.shouldBe (
      "1"
    )
  }

  it("Action with Literal -> JSON") {
    val action = Wget("http://dummy.org")

    val json = RecursiveMessageRelay.toM(action).prettyJSON()
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
    val json = RecursiveMessageRelay.toM(action).prettyJSON()
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
    val json = RecursiveMessageRelay.toM(action).prettyJSON()
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
    val json = RecursiveMessageRelay.toM(action).prettyJSON()
    json.shouldBe(
      """
        |{
        |  "children" : [ {
        |    "selector" : "o1",
        |    "delay" : "0 seconds",
        |    "blocking" : true
        |  }, {
        |    "filter" : { }
        |  } ],
        |  "limit" : 2147483647
        |}
      """.stripMargin
    )
  }
}
