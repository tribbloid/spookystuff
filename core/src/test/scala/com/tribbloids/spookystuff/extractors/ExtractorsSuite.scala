package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.SpookyEnvFixture

/**
  * Created by peng on 15/06/16.
  */
class ExtractorsSuite extends SpookyEnvFixture {

  test("Literal has the correct toString form") {
    val str = Literal("lit").toString
    assert(str == "\"lit\"")
  }
}
