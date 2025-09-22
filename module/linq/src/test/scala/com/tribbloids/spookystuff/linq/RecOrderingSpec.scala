package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.cap.Capability.<>
import ai.acyclic.prover.commons.testlib.BaseSpec
import com.tribbloids.spookystuff.linq.Rec
import com.tribbloids.spookystuff.linq.Foundation.^
import shapeless.test.illTyped

class RecOrderingSpec extends BaseSpec {

  import com.tribbloids.spookystuff.linq.Field.*

  describe("enabling") {

    it("for 1 field") {

      val r1 = ^(a = CanSort(1), b = CanSort("ab"))

      assert(r1.a == 1)
      r1.a: Int <> CanSort.type
    }

    it("for all fields") {

      val r1 = CanSort.row(^(a = 1, b = "ab"))

      assert(r1.a == 1)
      r1.a: Int <> CanSort.type

      val r2 = ^(c = 1.1) ++ r1
      r2.a: Int <> CanSort.type
      r2.c: Double

      illTyped(
        "r2.c: Double ^^ AffectOrdering"
      )
    }
  }

  it("summon") {

    val r1 = CanSort.row(^(a = 1, b = "ab"))

    val ordering = implicitly[Ordering[Rec[r1._internal.Repr]]]
  }

  it("Default") {
    val ordering = RowOrdering.Default

    val r1 = ^(a = 1)

    {
      val fn = ordering.at[r1._internal.Repr].Factory().fn
      fn(r1).runtimeList.mkString(",").shouldBe("()")
    }

    val r2 = CanSort.row(r1)

    {
      val fn = ordering.at[r2._internal.Repr].Factory().fn
      fn(r2).runtimeList.mkString(",").shouldBe("1")
    }

    val r3 = ^(b = 1.1) ++ r2

    {
      val fn = ordering.at[r3._internal.Repr].Factory().fn
      fn(r3).runtimeList.mkString(",").shouldBe("(),1")

      val oo = ordering.at[r3._internal.Repr].Factory().get
    }
  }
}
