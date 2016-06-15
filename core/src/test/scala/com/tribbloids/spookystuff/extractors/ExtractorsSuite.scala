package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.SpookyEnvSuite

/**
  * Created by peng on 15/06/16.
  */
class ExtractorsSuite extends SpookyEnvSuite {

  test("Literal has the correct toString form") {
    val str = Literal("lit").toString
    assert(str == "'lit'")
  }
}
