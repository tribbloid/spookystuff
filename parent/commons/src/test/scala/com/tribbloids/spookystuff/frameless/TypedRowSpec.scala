package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.testlib.BaseSpec
import ai.acyclic.prover.commons.util.Summoner
import com.tribbloids.spookystuff.testutils.TestHelper
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import shapeless.HList
import shapeless.record.Record
import shapeless.test.illTyped
import TypedRow.^

class TypedRowSpec extends BaseSpec {

  implicit def session: SparkSession = TestHelper.TestSparkSession

  it("Encoder") {

    implicitly[TypedRowSpec.RR <:< HList]

    implicitly[TypedEncoder[TypedRow[TypedRowSpec.RR]]]
    Summoner.summon[TypedEncoder[TypedRow[TypedRowSpec.RR]]]
  }

  describe("construction") {

    it("ofNamedArgs") {

      val t1 = ^(x = 1, y = "ab", z = 1.1)

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

  val xy = ^(x = 1, y = "ab")
  val yz = ^(y = 1.0, z = 1.1)
  val z1 = ^(z = 1.1)

  val z2 = ^(z = Seq(1.2, 1.3))

  describe("merge") {

    it("right") {

      val merged = xy +<+ yz

      assert(merged._internal.keys.runtimeList == List('x, 'y, 'z))
      assert(merged._internal.repr.runtimeList == List(1, 1.0, 1.1))

      assert(merged.y == 1.0) // favours the first operand
    }

    it("left") {

      val merged = xy +>+ yz

      assert(merged._internal.keys.runtimeList == List('x, 'y, 'z))
      assert(merged._internal.repr.runtimeList == List(1, "ab", 1.1))

      assert(merged.y == "ab") // favours the first operand
    }

    it("with duplicated keys") {

      illTyped("xy +!+ yz")

      val merged = xy +!+ z1

      assert(merged._internal.keys.runtimeList == List('x, 'y, 'z))
      assert(merged._internal.repr.runtimeList == List(1, "ab", 1.1))
    }

    describe("with selection") {

      it("right") {

        val merged = xy ++ yz.y

        assert(merged._internal.keys.runtimeList == List('x, 'y))
        assert(merged._internal.repr.runtimeList == List(1, 1.0))
      }
    }
  }

  val xys = Seq(xy, xy.update(y = "cd"))
  val yzs = Seq(yz, yz.update(y = 1.2))
  val z1s = Seq(z1, z1.update(z = 1.2))

  describe("cartesian product") {

    it("right") {

      val merged = xys ><< yzs

      merged
        .map(_._internal.runtimeVector)
        .mkString("\n")
        .shouldBe(
          """
            |Vector(1, 1.0, 1.1)
            |Vector(1, 1.2, 1.1)
            |Vector(1, 1.0, 1.1)
            |Vector(1, 1.2, 1.1)
            |""".stripMargin
        )

    }

    it("left") {

      val merged = xys >>< yzs

      merged
        .map(_._internal.runtimeVector)
        .mkString("\n")
        .shouldBe(
          """
            |Vector(1, ab, 1.1)
            |Vector(1, ab, 1.1)
            |Vector(1, cd, 1.1)
            |Vector(1, cd, 1.1)
            |""".stripMargin
        )
    }

    it("with duplicated keys") {

      illTyped("xy >!< yz")

      val merged = xys >!< z1s

      merged
        .map(_._internal.runtimeVector)
        .mkString("\n")
        .shouldBe(
          """
            |Vector(1, ab, 1.1)
            |Vector(1, ab, 1.2)
            |Vector(1, cd, 1.1)
            |Vector(1, cd, 1.2)
            |""".stripMargin
        )
    }

    describe("with selection") {

      it("right") {

        val selected = z2.z
        // yzs.map(_.y) won't work, deliberately

        val merged = xys >< selected

        merged
          .map(_._internal.runtimeVector)
          .mkString("\n")
          .shouldBe(
            """
              |Vector(1, ab, List(1.2, 1.3))
              |Vector(1, cd, List(1.2, 1.3))
              |""".stripMargin
          )
      }

      import TypedRow.functions._

      it(" ... exploded") {

        val exploded = {

          explode(
            z2.z
          )

          // which resolves to:
          //        val view = {
          //          Field.Named.asTypedRowView(
          //            z2.z
          //          )
          //        }
          //
          //        explode(
          //          view
          //        )
        }

        val merged = xys >< exploded

        merged
          .map(_._internal.runtimeVector)
          .mkString("\n")
          .shouldBe(
            """
              |Vector(1, ab, 1.2)
              |Vector(1, ab, 1.3)
              |Vector(1, cd, 1.2)
              |Vector(1, cd, 1.3)
              |""".stripMargin
          )
      }
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

  describe("_fields") {

    it("value") {

      val t1 = ^(x = 1, y = "ab")
      val col = t1._fields.y

      assert(col.value == "ab")
    }

    it("asTypedRow") {

      val t1 = ^(x = 1, y = "ab")
      val col = t1._fields.y

      val colAsRow = col.asTypedRow
      val colAsRowGT = ^(y = "ab")

      implicitly[colAsRow._internal.Repr =:= colAsRowGT._internal.Repr]

    }

    it("update") {

      val t1 = ^(x = 1, y = "ab")
      val col = t1._fields.y

      val t2 = col := 1.0
      val t2GT = t1 +<+ ^(y = 1.0)

      implicitly[t2._internal.Repr =:= t2GT._internal.Repr]

      assert(t2GT._internal.keys.runtimeList == List('x, 'y))

      assert(t2._internal.keys.runtimeList == List('x, 'y))

      assert(t2.y == 1.0)
    }

    //    it("remove") {}
  }

  describe("in Dataset") {

    it("named columns") {

      val t1 = ^(x = 1, y = "ab", z = 1.1)

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

    it(" ... with duplicated names") { // TODO: this should be an ill-posed problem

      val tx = ^(x = 1, y = "ab")
      val ty = ^(y = 1.0, z = 1.1)
      val t1 = TypedRowInternal.ofTuple(tx._internal.repr ++ ty._internal.repr)

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
