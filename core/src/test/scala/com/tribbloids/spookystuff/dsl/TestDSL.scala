package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.expressions.ExpressionLike
import com.tribbloids.spookystuff.rdd.PageRowRDD
import com.tribbloids.spookystuff.row.{DataRow, Field, SquashedPageRow}

/**
*  Created by peng on 12/3/14.
*/
class TestDSL extends SpookyEnvSuite {

//  import com.tribbloids.spookystuff.dsl._

  lazy val pages = (
    Wget("http://www.wikipedia.org/") ~ 'page  :: Nil
  ).fetch(spooky).toArray

  lazy val row = SquashedPageRow(dataRows = Array(DataRow()), fetchedOpt = Some(pages))
    .extract(
      S"title".head.text ~ 'abc,
      S"title".head ~ 'def
    )
      .unsquash.head

  test("symbol as Expr"){
    assert('abc.apply(row) === Some("Wikipedia"))
  }

  test("defaultAs should not rename an Alias") {
    val renamed = 'abc as 'name1
    assert(renamed.name == "name1")
    val renamed2 = renamed as 'name2
    assert(renamed2.name == "name2")
    val notRenamed = renamed defaultAs 'name2
    assert(notRenamed.name == "name1")
  }

  test("andThen"){
    val fun = 'abc.andThen(_.map(_.toString))
    assert(fun.name === "abc.<function1>")
    assert(fun(row) === Some("Wikipedia"))

    val fun2 = 'abc.andThen(ExpressionLike(_.map(_.toString), 'after))
    assert(fun2.name === "abc.after")
    assert(fun(row) === Some("Wikipedia"))
  }

  test("andMap"){
    val fun = 'abc.andMap(_.toString)
    assert(fun.name === "abc.<function1>")
    assert(fun(row) === Some("Wikipedia"))

    val fun2 = 'abc.andMap(_.toString, "after")
    assert(fun2.name === "abc.after")
    assert(fun(row) === Some("Wikipedia"))
  }

  test("andFlatMap"){
    val fun = 'abc.andFlatMap(_.toString.headOption)
    assert(fun.name === "abc.<function1>")
    assert(fun(row) === Some('W'))

    val fun2 = 'abc.andFlatMap(_.toString.headOption, "after")
    assert(fun2.name === "abc.after")
    assert(fun(row) === Some('W'))
  }

  test("double quotes in selector by attribute should work") {
    val pages = (
      Wget("http://www.wikipedia.org/") :: Nil
      ).fetch(spooky).toArray
    val row = SquashedPageRow(Array(DataRow()), fetchedOpt = Some(pages))
      .extract(S"""a[href*="wikipedia"]""".href ~ 'uri)
      .unsquash.head

    assert(row._1.get(Field("uri")).nonEmpty)
  }

  test("uri"){
    assert(S.uri.apply(row).get contains "://www.wikipedia.org/")
    assert('page.uri.apply(row).get contains "://www.wikipedia.org/")
    assert('def.uri.apply(row).get contains "://www.wikipedia.org/")
  }

  test("string interpolation") {
    val expr = x"static ${'notexist}"
    assert(expr.apply(row).isEmpty)
    assert(expr.orElse(" ").apply(row).get == " ")
  }

  test("SpookyContext can be cast to a blank PageRowRDD with empty schema") {
    val rdd = spooky: PageRowRDD
    assert(rdd.fields.isEmpty)
    assert(rdd.count() == 1)

    val plan = rdd.plan
    assert(plan.rdd() == spooky.blankSelfRDD)
    assert(plan.spooky != spooky) //configs should be deep copied
  }
}