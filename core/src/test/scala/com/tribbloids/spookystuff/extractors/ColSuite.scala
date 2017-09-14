package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.testutils.FunSpecx

/**
  * Created by peng on 14/07/17.
  */
class ColSuite extends FunSpecx {

  val c1: Col[String] = "abc"
  val c2: Col[String] = "abc"
  val cx: Col[String] = "123"
  val cNonLit: Col[String] = 'A

  it("Col(Lit).toString") {
    c1.toString.shouldBe(
      "abc"
    )
    cNonLit.toString.shouldBe(
      "Get('A)"
    )
  }

  it("Col(Lit).toMessage") {
    c1.proto.shouldBe(
      "abc"
    )
    intercept[UnsupportedOperationException] {
      cNonLit.proto
    }
  }

  it("Col(Lit) ==") {
    assert(c1 == c2)
    assert(c1 != cx)
    assert(cx != cNonLit)
  }
}