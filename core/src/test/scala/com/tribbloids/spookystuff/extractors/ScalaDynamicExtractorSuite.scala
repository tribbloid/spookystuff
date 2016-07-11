package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.tests.TestMixin
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import org.scalatest.FunSuite

/**
  * Created by peng on 09/07/16.
  */
class ScalaDynamicExtractorSuite extends FunSuite with TestMixin {

  test("can resolve type of String.startsWith(String)") {
    val ex = ScalaDynamicExtractor(
      Literal("abcde"),
      "startsWith",
      List(List(Literal("abc")))
    )

    assert(ex.resolveType(null) == BooleanType)
  }

  test("can resolve type of Array[String].length") {
    val ex = ScalaDynamicExtractor(
      Literal("a b c d e".split(" ")),
      "length",
      List()
    )

    assert(ex.resolveType(null) == IntegerType)
  }

  test("can resolve type of List[String].head") {
    val ex = ScalaDynamicExtractor(
      Literal("a b c d e".split(" ").toList),
      "head",
      List()
    )

    assert(ex.resolveType(null) == StringType)
  }

  test("can resolve type of Seq[String].head") {
    val ex = ScalaDynamicExtractor(
      Literal("a b c d e".split(" ").toSeq),
      "head",
      List()
    )

    assert(ex.resolveType(null) == StringType)
  }

  test("can resolve type of Doc.uri") {

  }
}
