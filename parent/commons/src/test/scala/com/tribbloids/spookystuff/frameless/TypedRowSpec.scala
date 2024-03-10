package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.testlib.BaseSpec
import ai.acyclic.prover.commons.util.Summoner
import com.tribbloids.spookystuff.testutils.TestHelper
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import shapeless.HList
import shapeless.record.Record
import shapeless.test.illTyped

class TypedRowSpec extends BaseSpec {

  implicit def session: SparkSession = TestHelper.TestSparkSession
  import TypedRow.Caps._

  it("Encoder") {

    implicitly[TypedRowSpec.RR <:< HList]

    implicitly[TypedEncoder[TypedRow[TypedRowSpec.RR]]]
    Summoner.summon[TypedEncoder[TypedRow[TypedRowSpec.RR]]]
  }

  it("construction") {
    val gd = Record(x = 1, y = "ab", z = 1.1)

    val t1 = TypedRow.ofRecord(x = 1, y = "ab", z = 1.1)
    assert(t1.asRepr == gd)

    val t2 = TypedRow.ofTuple(1, "ab", 1.1)
    assert(t2.asRepr == gd)
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

    assert(row.values.x == 1)
    assert(row.values.y == "ab")
    assert(row.values.z == 1.1)
  }

  describe("ordering") {

    it("enable") {

      val r1 = TypedRow.ofRecord(a = 1, b = "ab").enableOrdering

      assert(r1.values.a == 1)
      r1.values.a: Int ^^ AffectOrdering

      val r2 = TypedRow.ofRecord(c = 1.1) ++ r1
      r2.values.a: Int ^^ AffectOrdering
      r2.values.c: Double

      illTyped(
        "r2.c: Double ^^ AffectOrdering"
      )
    }

    it("native") {

      val r1 = TypedRow.ofRecord(a = 1)

      {
        val fn = TypedRow.For[r1.Repr].NativeOrdering().fn
        fn(r1).runtimeList.mkString(",").shouldBe("()")
      }

      val r2 = r1.enableOrdering

      {
        val fn = TypedRow.For[r2.Repr].NativeOrdering().fn
        fn(r2).runtimeList.mkString(",").shouldBe("1")
      }

      val r3 = TypedRow.ofRecord(b = 1.1) ++ r2

      {
        val fn = TypedRow.For[r3.Repr].NativeOrdering().fn
        fn(r3).runtimeList.mkString(",").shouldBe("(),1")
      }
    }
  }

}

object TypedRowSpec {

  val RR = Record.`'x -> Int, 'y -> String`
  type RR = RR.T
}
