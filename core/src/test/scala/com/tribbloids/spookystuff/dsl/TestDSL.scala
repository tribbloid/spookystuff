package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.extractors.Alias
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row.{DataRow, FetchedRow, Field, SquashedFetchedRow}
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture

/**
  *  Created by peng on 12/3/14.
  */
class TestDSL extends SpookyEnvFixture with LocalPathDocsFixture {

  lazy val pages = (
    Wget(HTML_URL) ~ 'page :: Nil
  ).fetch(spooky)

  lazy val row = SquashedFetchedRow
    .withDocs(dataRows = Array(DataRow()), docs = pages)
    .extract(
      S"title".head.text withAlias 'abc,
      S"title".head withAlias 'def
    )
    .unsquash
    .head

  it("symbol as Expr") {
    assert('abc.resolve(emptySchema).apply(row) === "Wikipedia")
  }

  it("defaultAs should not rename an Alias") {
    val renamed = 'abc as 'name1
    assert(renamed.asInstanceOf[Alias[_, _]].field.name == "name1")
    val renamed2 = renamed as 'name2
    assert(renamed2.asInstanceOf[Alias[_, _]].field.name == "name2")
    val notRenamed = renamed withAliasIfMissing 'name2
    assert(notRenamed.field.name == "name1")
  }

  it("andFn") {
    val fun = 'abc.andFn(_.toString).resolve(emptySchema)
//    assert(fun.toString === "<function1>")
    assert(fun(row) === "Wikipedia")
  }

  it("andUnlift") {
    val fun = 'abc.andOptionFn(_.toString.headOption).resolve(emptySchema)
//    assert(fun.toString === "<function1>")
    assert(fun(row) === 'W')
  }

  it("double quotes in selector by attribute should work") {
    val pages = (
      Wget(HTML_URL) :: Nil
    ).fetch(spooky).toArray
    val row = SquashedFetchedRow
      .withDocs(Array(DataRow()), docs = pages)
      .extract(S"""a[href*="wikipedia"]""".href withAlias 'uri)
      .unsquash
      .head

    assert(row.dataRow.get(Field("uri")).nonEmpty)
  }

  it("uri") {
    assert(S.uri.apply(row) contains HTML_URL)
    assert('page.uri.apply(row) contains HTML_URL)
    assert('def.uri.apply(row) contains HTML_URL)
  }

  it("string interpolation") {
    val expr = x"static ${'notexist}".resolve(emptySchema)
    assert(expr.lift.apply(row).isEmpty)
    assert(expr.orElse[FetchedRow, String] { case _ => " " }.apply(row) == " ")
  }

  it("SpookyContext can be cast to a blank PageRowRDD with empty schema") {
    val rdd = spooky: FetchedDataset
    assert(rdd.fields.isEmpty)
    assert(rdd.count() == 1)

    val plan = rdd.plan
    assert(plan.rdd() == spooky._blankSelfRDD)
    assert(plan.spooky != spooky) //configs should be deep copied
  }
}
