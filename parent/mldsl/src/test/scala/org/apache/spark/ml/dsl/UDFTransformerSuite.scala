package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.testutils.{BaseSpec, TestHelper}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

case class User(
    name: String,
    age: Int
)

class UDFTransformerSuite extends BaseSpec {

  val df1 = TestHelper.TestSQL.createDataFrame(
    Seq(
      User("Reza$", 25),
      User("Holden$", 25)
    )
  )

  val tokenizer: Tokenizer = new Tokenizer().setInputCol("name").setOutputCol("name_token")
  val stemming: UserDefinedFunction = udf { v: Seq[String] =>
    v.map(_.stripSuffix("$"))
  }
  val arch = UDFTransformer().setUDFSafely(stemming).setInputCols(Array("name_token")).setOutputCol("name_stemmed")
  val src: DataFrame = tokenizer.transform(df1)

  it("transformer has consistent schema") {
    val end = arch.transform(src)
    val endSchema = end.schema
    val endSchema2 = arch.transformSchema(src.schema)
    assert(endSchema.toString() == endSchema2.toString())
  }

  it("transformer can add new column") {
    val end = arch.transform(src)
    end
      .collect()
      .mkString("\n")
      .shouldBe(
        """
        |[Reza$,25,ArraySeq(reza$),ArraySeq(reza)]
        |[Holden$,25,ArraySeq(holden$),ArraySeq(holden)]
        |""".stripMargin
      )
  }
}
