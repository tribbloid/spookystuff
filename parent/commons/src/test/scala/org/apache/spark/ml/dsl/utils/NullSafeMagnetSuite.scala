package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.testutils.BaseSpec

class NullSafeMagnetSuite extends BaseSpec {

  val v: String = "abc"

  def validate(nullSafe: => NullSafeMagnet[String]): NullSafeMagnet[String] = {

    assert(nullSafe.asOption.get == v)
    nullSafe
  }

  it("can be converted from option") {

    validate(Some("abc"): String ?? _)
  }

  it("can be converted from value") {

    validate("abc": String ?? _)
  }

  it("CannotBeNull can only be converted from Some") {

    validate(Some("abc"): String !! _)

    // this will fail
//    validate(Option("abc"): String ! _)
  }
}
