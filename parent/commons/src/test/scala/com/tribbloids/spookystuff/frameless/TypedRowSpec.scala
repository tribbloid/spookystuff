package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.testlib.BaseSpec
import ai.acyclic.prover.commons.util.Summoner
import com.tribbloids.spookystuff.testutils.TestHelper
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import shapeless.HList
import shapeless.record.Record

class TypedRowSpec extends BaseSpec {

  implicit lazy val session: SparkSession = TestHelper.TestSparkSession

  it("Encoder") {

    implicitly[TypedRowSpec.RR <:< HList]

    implicitly[TypedEncoder[TypedRow[TypedRowSpec.RR]]]
    Summoner.summon[TypedEncoder[TypedRow[TypedRowSpec.RR]]]
  }

  it("in Dataset") {

    val r1 = Record(x = 1, y = "ab", z = 1.1)
    val r2 = TypedRow.fromHList(r1)

    val rdd = session.sparkContext.parallelize(Seq(r2))
    val ds = TypedDataset.create(rdd)

    ds.schema.treeString.shouldBe(
      """
        |root
        | |-- x: integer (nullable = true)
        | |-- y: string (nullable = true)
        | |-- z: double (nullable = true)
        |""".stripMargin
    )

    assert(ds.toDF().collect().head.toString() == "[1,ab,1.1]")

    val row = ds.dataset.collect.head
    assert(row == r2)

    assert(row.x == 1)
    assert(row.y == "ab")
    assert(row.z == 1.1)
  }
}

object TypedRowSpec {

  val RR = Record.`'x -> Int, 'y -> String`
  type RR = RR.T
}
