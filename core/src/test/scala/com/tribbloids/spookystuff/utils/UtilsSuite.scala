package com.tribbloids.spookystuff.utils

import org.scalatest.FunSuite

/**
 * Created by peng on 11/1/14.
 */
class UtilsSuite extends FunSuite {

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
}
