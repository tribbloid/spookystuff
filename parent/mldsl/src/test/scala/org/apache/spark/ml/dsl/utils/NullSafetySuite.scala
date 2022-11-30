package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.testutils.FunSpecx

class NullSafetySuite extends FunSpecx {

  val v = "abc"

  def validate(nullSafe: => NullSafety.Magnet[String]): NullSafety.Magnet[String] = {

    assert(nullSafe.asOption.get == v)
    nullSafe
  }

  it("can be converted from option") {

    validate(Some("abc"): String `?` _)
  }

  it("can be converted from value") {

    validate("abc": String `?` _)
  }

  it("CannotBeNull can only be converted from Some") {

    validate(Some("abc"): String ! _)

    // this will fail
//    validate(Option("abc"): String ! _)
  }

  it("String ? Var supports mutation") {
    validate {
      val v: String `?` Var = "def"
      `v` := "abc"
      v
    }

    // this will fail
//    val v: String ? _ = "def"
//    v := "abc"
  }
}
