package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.testlib.BaseSpec
import ai.acyclic.prover.commons.util.Summoner
import com.tribbloids.spookystuff.testutils.TestHelper
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import shapeless.HList
import shapeless.record.Record

class TypedRowSpec extends BaseSpec {

  implicit def session: SparkSession = TestHelper.TestSparkSession

  it("Encoder") {

    implicitly[TypedRowSpec.RR <:< HList]

    implicitly[TypedEncoder[TypedRow[TypedRowSpec.RR]]]
    Summoner.summon[TypedEncoder[TypedRow[TypedRowSpec.RR]]]
  }

  describe("construction") {

    it("ofNamedArgs") {

      val t1 = TypedRow(x = 1, y = "ab", z = 1.1)

      val gd = HList(1, "ab", 1.1)
      assert(t1._internal.repr == gd)

      //    TypeViz[t1.Repr].toString.shouldBe()
    }

//    it("ofTuple") {
//
//      val gd = HList(1, "ab", 1.1)
//      val t1 = TypedRowInternal.ofTuple(gd)
//      assert(t1.repr == gd)
//
////      val rdd = session.sparkContext.parallelize(Seq(t1))
//      val ds = TypedDataset.create(Seq(t1))
//    }
  }

  describe("merge") {

    it("mayCauseDuplicatedNames") {

      val t1 = TypedRow(x = 1, y = "ab")
      val t2 = TypedRow(y = 1.0, z = 1.1)
      val merged = t1 ++ t2

      assert(merged._internal.keys.runtimeList == List('x, 'y, 'y, 'z))
      assert(merged._internal.repr.runtimeList == List(1, "ab", 1.0, 1.1))

      assert(t1.x == 1)
      assert(merged.x == 1)
      assert(merged.y == "ab") // favours the first operand
    }

    it("right") {

      val t1 = TypedRow(x = 1, y = "ab")
      val t2 = TypedRow(y = 1.0, z = 1.1)
      val merged = t1 ++< t2

      assert(merged._internal.keys.runtimeList == List('x, 'y, 'z))
      assert(merged._internal.repr.runtimeList == List(1, 1.0, 1.1))

      assert(merged.y == 1.0) // favours the first operand
    }

    it("left") {

      val t1 = TypedRow(x = 1, y = "ab")
      val t2 = TypedRow(y = 1.0, z = 1.1)
      val merged = t1 >++ t2

      assert(merged._internal.keys.runtimeList == List('x, 'y, 'z))
      assert(merged._internal.repr.runtimeList == List(1, "ab", 1.1))

      assert(merged.y == "ab") // favours the first operand
    }
  }

  //  it("removeAll") {
  //    // TODO implementations of records.Updater/Update/UpdateAll are all defective due to macro
  //
  //    val t1 = TypedRow.ofNamedArgs(x = 1, y = "ab", z = 1.1)
  ////    val r1 = t1.removeAll(Columns("x", "y"))
  //
  //    implicitly[
  //      Remover[
  //        t1.Repr,
  //        Col["x"]
  //      ]
  //    ]
  //
  //    implicitly[
  //      RemoveAll[
  //        t1.Repr,
  //        Col["x"] :: HNil
  //      ]
  //    ]
  //
  ////    assert(r1.keys.runtimeList == List())
  //
  //  }

  describe("columns") {

    it("value") {

      val t1 = TypedRow(x = 1, y = "ab")
      val col = t1._fields.y

      assert(col.value == "ab")
    }

    it("asTypedRow") {

      val t1 = TypedRow(x = 1, y = "ab")
      val col = t1._fields.y

      val colAsRow = col.asTypedRow
      val colAsRowGT = TypedRow(y = "ab")

      implicitly[colAsRow._internal.Repr =:= colAsRowGT._internal.Repr]

    }

    it("update") {

      val t1 = TypedRow(x = 1, y = "ab")
      val col = t1._fields.y

      val t2 = col := 1.0
      val t2GT = t1 ++< TypedRow(y = 1.0)

      implicitly[t2._internal.Repr =:= t2GT._internal.Repr]

      assert(t2GT._internal.keys.runtimeList == List('x, 'y))

      assert(t2._internal.keys.runtimeList == List('x, 'y))

      assert(t2.y == 1.0)
    }

    //    it("remove") {}

  }

  describe("in Dataset") {

    it("named columns") {

      val t1 = TypedRow(x = 1, y = "ab", z = 1.1)

      val rdd = session.sparkContext.parallelize(Seq(t1))
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
      assert(row == row)

      assert(row.x == 1)
      assert(row.y == "ab")
      assert(row.z == 1.1)
    }

    it(" ... with duplicated names") {

      val tx = TypedRow(x = 1, y = "ab")
      val ty = TypedRow(y = 1.0, z = 1.1)
      val t1 = tx ++ ty

      val rdd = session.sparkContext.parallelize(Seq(t1))
      val ds = TypedDataset.create(rdd)

      ds.schema.treeString.shouldBe(
        """
          |root
          | |-- x: integer (nullable = true)
          | |-- y: string (nullable = true)
          | |-- y: double (nullable = true)
          | |-- z: double (nullable = true)
          |""".stripMargin
      )
    }
  }

}

object TypedRowSpec {

  val RR = Record.`'x -> Int, 'y -> String`
  type RR = RR.T
}
