package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.testlib.BaseSpec
import shapeless.test.illTyped

class TypedRowOrderingSpec extends BaseSpec {

  import com.tribbloids.spookystuff.frameless.Field._

  describe("enabling") {

    it("for 1 field") {

      val r1 = TypedRow.ProductView(a = CanSort(1), b = CanSort("ab"))

      assert(r1.values.a == 1)
      r1.values.a: Int ^^ CanSort
    }

    it("for all fields") {

      val r1 = CanSort(TypedRow.ProductView(a = 1, b = "ab"))

      assert(r1.values.a == 1)
      r1.values.a: Int ^^ CanSort

      val r2 = TypedRow.ProductView(c = 1.1) ++ r1
      r2.values.a: Int ^^ CanSort
      r2.values.c: Double

      illTyped(
        "r2.c: Double ^^ AffectOrdering"
      )
    }
  }

  it("summon") {

    val r1 = CanSort(TypedRow.ProductView(a = 1, b = "ab"))

    val ordering = implicitly[Ordering[TypedRow[r1._internal.Repr]]]
  }

  it("Default") {
    val ordering = TypedRowOrdering.Default

    val r1 = TypedRow.ProductView(a = 1)

    {
      val fn = ordering.at[r1._internal.Repr].Factory().fn
      fn(r1).runtimeList.mkString(",").shouldBe("()")
    }

    val r2 = CanSort(r1)

    {
      val fn = ordering.at[r2._internal.Repr].Factory().fn
      fn(r2).runtimeList.mkString(",").shouldBe("1")
    }

    val r3 = TypedRow.ProductView(b = 1.1) ++ r2

    {
      val fn = ordering.at[r3._internal.Repr].Factory().fn
      fn(r3).runtimeList.mkString(",").shouldBe("(),1")

      val oo = ordering.at[r3._internal.Repr].Factory().get
    }
  }
}
