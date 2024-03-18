package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.testlib.BaseSpec
import shapeless.test.illTyped

class TypedRowOrderingSpec extends BaseSpec {

  import com.tribbloids.spookystuff.frameless.Field._

  describe("enabling") {

    it("for 1 field") {

      val r1 = TypedRow.ofNamedArgs(a = CanSort(1), b = CanSort("ab"))

      assert(r1.values.a == 1)
      r1.values.a: Int ^^ CanSort
    }

    it("for all fields") {

      val r1 = TypedRow.ofNamedArgs(a = 1, b = "ab").canSortAll

      assert(r1.values.a == 1)
      r1.values.a: Int ^^ CanSort

      val r2 = TypedRow.ofNamedArgs(c = 1.1) ++ r1
      r2.values.a: Int ^^ CanSort
      r2.values.c: Double

      illTyped(
        "r2.c: Double ^^ AffectOrdering"
      )
    }
  }

  it("summon") {

    val r1 = TypedRow.ofNamedArgs(a = 1, b = "ab").canSortAll

    val ordering = implicitly[Ordering[TypedRow[r1.Repr]]]
  }

  it("Default") {
    val ordering = TypedRowOrdering.Default

    val r1 = TypedRow.ofNamedArgs(a = 1)

    {
      val fn = ordering.at[r1.Repr].Factory().fn
      fn(r1).runtimeList.mkString(",").shouldBe("()")
    }

    val r2 = r1.canSortAll

    {
      val fn = ordering.at[r2.Repr].Factory().fn
      fn(r2).runtimeList.mkString(",").shouldBe("1")
    }

    val r3 = TypedRow.ofNamedArgs(b = 1.1) ++ r2

    {
      val fn = ordering.at[r3.Repr].Factory().fn
      fn(r3).runtimeList.mkString(",").shouldBe("(),1")

      val oo = ordering.at[r3.Repr].Factory().get
    }
  }
}
