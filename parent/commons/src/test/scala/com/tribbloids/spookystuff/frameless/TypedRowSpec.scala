package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.testlib.BaseSpec
import ai.acyclic.prover.commons.util.Summoner
import ai.acyclic.prover.commons.viz.TypeViz
import com.tribbloids.spookystuff.testutils.TestHelper
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import shapeless.HList
import shapeless.record.Record

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
    assert(t1.asRecord == gd)

    val t2 = TypedRow.ofTuple(1, "ab", 1.1)
    assert(t2.asRecord == gd)
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

  describe("ordering") {

    it("enable") {

      val r1 = TypedRow.ofRecord(a = 1, b = "ab").enableOrdering

      assert(r1.a == 1)
      r1.a: Int ^^ AffectOrdering

      val r2 = TypedRow.ofRecord(c = 1.1) ++ r1
      r2.a: Int ^^ AffectOrdering
      r2.c: Double

//      r2.c: Double ^^ AffectOrdering

    }

    it("native") {

      val r1 = TypedRow.ofRecord(a = 1)

      {
        val fn = TypedRow.For[r1.Repr].NativeOrdering().fn

        fn(r1).runtimeList.mkString(",").shouldBe("")
      }

      val r2 = r1.enableOrdering

//      {
//
//        val resolving = TypedRow.For[r2.Repr].NativeOrdering()
//
//        TypeViz[resolving.Values].diagram_hierarchy.toString.shouldBe()
//
//        TypeViz[resolving.Mapped].diagram_hierarchy.toString.shouldBe()
//
//        val fn = resolving.fn
//
//        val result = fn(r2)
//
//        fn(r2).runtimeList.mkString(",").shouldBe("")
//      }
    }
  }

}

object TypedRowSpec {

  val RR = Record.`'x -> Int, 'y -> String`
  type RR = RR.T
}
