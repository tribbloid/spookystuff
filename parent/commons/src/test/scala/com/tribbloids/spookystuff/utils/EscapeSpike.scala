package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.BaseSpec
import org.apache.commons.text.StringEscapeUtils

class EscapeSpike extends BaseSpec {

  it("A") {

    println(StringEscapeUtils.unescapeJava("apply$mcI$sp"))
  }
}
