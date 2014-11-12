package org.tribbloid.spookystuff.utils

import org.scalatest.FunSuite

/**
 * Created by peng on 11/1/14.
 */
class TestUtils extends FunSuite {

  test("Clean ?:$&#"){
    val url = Utils.canonizeUrn("http://abc.com?re#k2$si")
    assert(url === "http/abc.com/re/k2/si")
  }

  test("interpolate") {
    val someMap = Map("abc" -> 1, "def" -> 2.2)
    val result = Utils.interpolateFromMap("rpk#{abc}aek#{def}", someMap)
    assert(result === "rpk1aek2.2")
  }

  test("interpolate throws an exception when key not found") {
    var thrown = false
    try {
      val someMap = Map("abc" -> 1, "def" -> 2.2)
      val result = Utils.interpolateFromMap("rpk#{abc}aek#{def}", someMap)
    }
    catch {
      case e: Throwable => thrown = true
    }
    assert(thrown === true)
  }
}
