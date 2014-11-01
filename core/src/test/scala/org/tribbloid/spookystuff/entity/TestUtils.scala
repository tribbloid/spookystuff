package org.tribbloid.spookystuff.entity

import org.scalatest.FunSuite
import org.tribbloid.spookystuff.Utils

/**
 * Created by peng on 11/1/14.
 */
class TestUtils extends FunSuite {

  test("Clean ?:$&#"){
    val url = Utils.canonizeUrn("http://abc.com?re#k2$si")
    assert(url === "http/abc.com/re/k2/si")
  }
}
