package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.doc.DocOption
import com.tribbloids.spookystuff.extractors.HasAlias
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row.{Alias, DataRow, FetchedRow, SquashedRow}
import com.tribbloids.spookystuff.testutils.{LocalPathDocsFixture, SpookyBaseSpec}

/**
  * Created by peng on 12/3/14.
  */
class TestDSL extends SpookyBaseSpec with LocalPathDocsFixture {

  lazy val pages: Seq[DocOption] = (
    Wget(HTML_URL) ~ 'page
  ).fetch(spooky)

  lazy val row: FetchedRow = SquashedRow
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
    val renamed = 'abc ~ 'name1
    assert(renamed.asInstanceOf[HasAlias[_, _]].alias.name == "name1")
    val renamed2 = renamed ~ 'name2
    assert(renamed2.asInstanceOf[HasAlias[_, _]].alias.name == "name2")
    val notRenamed = renamed withAliasIfMissing 'name2
    assert(notRenamed.alias.name == "name1")
  }

  it("andFn") {
    val fun = 'abc.andMap(_.toString).resolve(emptySchema)
//    assert(fun.toString === "<function1>")
    assert(fun(row) === "Wikipedia")
  }

  it("andUnlift") {
    val fun = 'abc.andFlatMap(_.toString.headOption).resolve(emptySchema)
//    assert(fun.toString === "<function1>")
    assert(fun(row) === 'W')
  }

  it("double quotes in selector by attribute should work") {
    val pages = Wget(HTML_URL).fetch(spooky).toArray
    val row = SquashedRow
      .withDocs(Array(DataRow()), docs = pages)
      .extract(S"""a[href*="wikipedia"]""".href withAlias 'uri)
      .unsquash
      .head

    assert(row.dataRow.get(Alias("uri")).nonEmpty)
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
    val ds = spooky: FetchedDataset
    assert(ds.schema.fields.isEmpty)
    assert(ds.count() == 1)

    val plan = ds.plan
    assert(plan.rdd() == spooky._blankRowRDD)
    assert(!(plan.spooky eq spooky)) // configs should be deep copied
  }
}
