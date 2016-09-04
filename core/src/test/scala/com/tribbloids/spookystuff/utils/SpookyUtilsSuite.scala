package com.tribbloids.spookystuff.utils

import org.scalatest.FunSuite

/**
 * Created by peng on 11/1/14.
 */
class SpookyUtilsSuite extends FunSuite {

  test("canonizeUrn should clean ?:$&#"){
    val url = SpookyUtils.canonizeUrn("http://abc.com?re#k2$si")
    assert(url === "http/abc.com/re/k2/si")
  }

  test("asArray[Int]") {
    assert(SpookyUtils.asArray[Int](2).toSeq == Seq(2))
    assert(SpookyUtils.asArray[Int](Seq(1,2,3).iterator).toSeq == Seq(1,2,3))
    assert(SpookyUtils.asArray[Int](Seq(1, 2.2, "b")).toSeq == Seq(1))
  }


  test("asIterable[Int]") {
    assert(SpookyUtils.asIterable[Int](2) == Iterable(2))
    assert(SpookyUtils.asIterable[Int](Seq(1,2,3).iterator).toSeq == Iterable(1,2,3))
    assert(SpookyUtils.asIterable[Int](Seq(1, 2.2, "b")).toSeq == Iterable(1))
  }

  test("copyResourceToDirectory can extract a dependency's package in a jar") {
    val src = SpookyUtils.getCPResource("org/apache/log4j/xml").get
    val dst = "temp/log4j"
    SpookyUtils.extractResource(src, dst)
    null
  }

  test("copyResourceToDirectory can extract a package in file system") {
    val src = SpookyUtils.getCPResource("com/tribbloids/spookystuff/utils").get
    val dst = "temp/utils"
    SpookyUtils.extractResource(src, dst)
    null
  }
}
