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
//    val k = cNonLit.ex.memberStr
//    val msg = cNonLit.message
//    val writer = cNonLit: MessageWriter[_]
//    val mem = cNonLit.memberStr
    cNonLit.toString.shouldBe(
      "Get('A)"
    )
  }

  it("Col(Lit).value") {
    c1.value.shouldBe(
      "abc"
    )
    intercept[UnsupportedOperationException] {
      cNonLit.value
    }
  }

  it("Col(Lit) ==") {
    assert(c1 == c2)
    assert(c1 != cx)
    assert(cx != cNonLit)
  }
}
