package com.tribbloids.spookystuff.parser

import org.scalatest.FunSpec

/**
  * Created by peng on 23/01/17.
  */
class TestOptionsParsers extends FunSpec {

  val parsers = OptionsParsers

  import parsers._

  it("paramsParser can treat line break as whitespace character") {

    val result = apply(
      """
        |a=1
        |b='2'
        |c=3
      """.stripMargin)
    assert(
      result.toSeq.mkString("\n") ==
        """
          |(a,Some(1))
          |(b,Some(2))
          |(c,Some(3))
        """.trim.stripMargin
    )
  }
}
