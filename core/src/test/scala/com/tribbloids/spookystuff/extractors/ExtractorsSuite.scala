package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.Wget
import org.apache.spark.ml.dsl.utils.MessageView
import org.apache.spark.sql.types.NullType

//case class Lit[T, +R](value: R, dataType: DataType = NullType)
/**
  * Created by peng on 15/06/16.
  */
class ExtractorsSuite extends SpookyEnvFixture {

  test("Literal can be converted to JSON") {
    val lit: Literal[FR, Int] = Literal(1)

    val json = lit.toJSON()
    json.shouldBe (
      "1"
    )
  }

  test("Action that use Literal can be converted to JSON") {
    val action = Wget(Literal("http://dummy.org"))

    val json = action.toMessage.toJSON()
    json.shouldBe(
      """
        |{
        |  "className" : "com.tribbloids.spookystuff.actions.Wget",
        |  "params" : {
        |    "uri" : "http://dummy.org",
        |    "filter" : { }
        |  }
        |}
      """.stripMargin
    )
  }

  test("Literal has the correct toString form") {
    val str = Literal("lit").toString
    assert(str == "\"lit\"")
  }
}
