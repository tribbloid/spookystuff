package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.row.{Field, DataRow}
import org.scalatest.FunSuite

import scala.collection.immutable.ListMap

/**
 * Created by peng on 11/1/14.
 */
class TestUtils extends FunSuite {

  test("Clean ?:$&#"){
    val url = Utils.canonizeUrn("http://abc.com?re#k2$si")
    assert(url === "http/abc.com/re/k2/si")
  }

  test("asArray[Int]") {
    assert(Utils.asArray[Int](2).toSeq == Seq(2))
    assert(Utils.asArray[Int](Seq(1,2,3).iterator).toSeq == Seq(1,2,3))
    assert(Utils.asArray[Int](Seq(1, 2.2, "b")).toSeq == Seq(1))
  }


  test("asIterable[Int]") {
    assert(Utils.asIterable[Int](2) == Iterable(2))
    assert(Utils.asIterable[Int](Seq(1,2,3).iterator).toSeq == Iterable(1,2,3))
    assert(Utils.asIterable[Int](Seq(1, 2.2, "b")).toSeq == Iterable(1))
  }
}
