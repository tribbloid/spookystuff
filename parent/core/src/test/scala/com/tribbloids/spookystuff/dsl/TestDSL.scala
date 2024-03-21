package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.extractors.Alias
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row.{Lineage, FetchedRow, Field, SquashedRow}
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

/**
  * Created by peng on 12/3/14.
  */
class TestDSL extends SpookyBaseSpec with FileDocsFixture {

  lazy val pages: Seq[Observation] = (
    Wget(HTML_URL) ~ 'page
  ).fetch(spooky)

  lazy val row: FetchedRow = SquashedRow
    .ofData(Lineage.blank)
    .cache(pages)
    .withCtx(spooky)
    .resetScope
    .extract(
      S"title".head.text withAlias 'abc,
      S"title".head withAlias 'def
    )
    .withCtx(spooky)
    .unSquash
    .head

  it("symbol as Expr") {
    assert('abc.resolve(emptySchema).apply(row) === "Wikipedia")
  }

  it("defaultAs should not rename an Alias") {
    val renamed = 'abc as 'name1
    assert(renamed.asInstanceOf[Alias[_, _]].field.name == "name1")
    val renamed2 = renamed as 'name2
    assert(renamed2.asInstanceOf[Alias[_, _]].field.name == "name2")
    val notRenamed = renamed withFieldIfMissing 'name2
    assert(notRenamed.field.name == "name1")
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
      .ofData(Lineage.blank)
      .cache(pages)
      .withCtx(spooky)
      .resetScope
      .extract(S"""a[href*="wikipedia"]""".href withAlias 'uri)
      .withCtx(spooky)
      .unSquash
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

  it("SpookyContext can be cast to a blank RDD with empty schema") {
    val ds = spooky: FetchedDataset
    assert(ds.fields.isEmpty)
    assert(ds.count() == 1)

    val plan = ds.plan
    val rdd = plan.squashedRDD
    assert(rdd.collect().toSeq == Seq(SquashedRow.ofData(Lineage.blank)))
    assert(ds.schema.fields == Nil)
    assert(!(plan.spooky eq spooky)) // configs should be deep copied
  }
}
