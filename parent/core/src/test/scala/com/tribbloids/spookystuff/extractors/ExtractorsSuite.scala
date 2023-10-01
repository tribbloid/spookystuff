package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec

/**
  * Created by peng on 15/06/16.
  */
class ExtractorsSuite extends SpookyBaseSpec {

  it("Literal -> JSON") {
    val lit: Lit[FR, Int] = Lit(1)

    val json = lit.prettyJSON()
    json.shouldBe(
      "1"
    )
  }

  it("Literal.toString") {
    val str = Lit("lit").toString
    assert(str == "lit")
  }
}
