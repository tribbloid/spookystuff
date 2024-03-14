package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.testlib.BaseSpec

class TypedRowOrderingSpec extends BaseSpec {

  it("Default") {
    val ordering = TypedRowOrdering.Default

    val r1 = TypedRow.ofNamedArgs(a = 1)

    {
      val fn = ordering.at[r1.Repr].Factory().fn
      fn(r1).runtimeList.mkString(",").shouldBe("()")
    }

    val r2 = r1.enableOrdering

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
