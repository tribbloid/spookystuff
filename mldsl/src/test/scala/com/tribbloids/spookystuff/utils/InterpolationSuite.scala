package com.tribbloids.spookystuff.utils

import org.scalatest.FunSpec

class InterpolationSuite extends FunSpec {

  val ii: Interpolation = Interpolation.`$`

  it("can interpolate delimited field") {

    assert(
      ii.apply("abc${def}ghi") { s =>
        s + "0"
      } == "abcdef0ghi"
    )
  }

  it("can avoid escaped char") {

    assert(
      ii.apply("abc$${def}ghi") { s =>
        s + "0"
      } == "abc${def}ghi"
    )
  }

  it("can avoid continuous escaped chars") {
    assert(
      ii.apply("abc$$$$$${def}ghi") { s =>
        s + "0"
      } == "abc$$${def}ghi"
    )
  }

  it("can identify delimiter after continuous escaped chars") {
    assert(
      ii.apply("abc$$$$${def}ghi") { s =>
        s + "0"
      } == "abc$$def0ghi"
    )
  }
}
