package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.Extractors._
import com.tribbloids.spookystuff.extractors.impl.{Get, Interpolate}
import com.tribbloids.spookystuff.row.Field

import scala.language.implicitConversions

/**
  * this hierarchy aims to create a short DSL for selecting components from PageRow, e.g.: 'abc: cells with key "abc",
  * tempkey precedes ordinary key 'abc.S("div#a1"): all children of an unstructured field (either a page or element)
  * that match the selector S("div#a1"): all children of the only page that match the selector, if multiple page per
  * row, throws an exception S_*("div#a1"): all children of all pages that match the selector. 'abc.S("div#a1").head:
  * first child of an unstructured field (either a page or element) that match the selector 'abc.S("div#a1").text: first
  * text of an unstructured field that match the selector 'abc.S("div#a1").texts: all texts of an unstructured field
  * that match the selector 'abc.S("div#a1").attr("src"): first "src" attribute of an unstructured field that match the
  * selector 'abc.S("div#a1").attrs("src"): first "src" attribute of an unstructured field that match the selector
  */
sealed trait Level2 {

  import GenExtractor._

  // --------------------------------------------------

  implicit def symbol2Field(symbol: Symbol): Field =
    Option(symbol).map(v => Field(v.name)).orNull

  implicit def symbol2Get(symbol: Symbol): Get =
    Get(symbol.name)

  implicit def symbolToDocExView(symbol: Symbol): DocExView =
    GetDocExpr(symbol.name)

  implicit def symbol2GetItr(symbol: Symbol): IterableExView[Any] =
    IterableExView(Get(symbol.name).GetSeq)
}

sealed trait Level1 extends Level2 {

  import GenExtractor._

  implicit def symbol2GetUnstructured(symbol: Symbol): UnstructuredExView =
    GetUnstructuredExpr(symbol.name)

  implicit class StrContextHelper(val strC: StringContext) extends Serializable {

    def x(parts: Col[String]*): Interpolate = Interpolate(strC.parts, parts.map(_.ex))

    def CSS(parts: Col[String]*): GenExtractor[FR, Elements[Unstructured]] =
      GetOnlyDocExpr.andFn(_.root).findAll(strC.s(parts: _*))
    def S(parts: Col[String]*): GenExtractor[FR, Elements[Unstructured]] = CSS(parts: _*)

    def CSS_*(parts: Col[String]*) = GetAllDocsExpr.findAll(strC.s(parts: _*))
    def S_*(parts: Col[String]*) = CSS_*(parts: _*)

    def A(parts: Col[String]*): GenExtractor[FR, Elements[Unstructured]] = 'A.findAll(strC.s(parts: _*))
  }
}

class DSL extends Level1 {

  import com.tribbloids.spookystuff.extractors.impl.Extractors._

  def S: GenExtractor[FR, Doc] = GetOnlyDocExpr
  def S(selector: String): GenExtractor[FR, Elements[Unstructured]] = S.andFn(_.root).findAll(selector)
  //  def S(selector: String, i: Int): Extractor[Unstructured] = {
  //    val expr = this.S(selector)
  //    new IterableExView(expr).get(i)
  //  }
  def `S_*`: GenExtractor[FR, Elements[Unstructured]] = GetAllDocsExpr
  def S_*(selector: String): GenExtractor[FR, Elements[Unstructured]] = `S_*`.findAll(selector)
  //  def S_*(selector: String, i: Int): Extractor[Unstructured] = {
  //    val expr = GetAllPagesExpr.findAll(selector)
  //    new IterableExView(expr).get(i)
  //  }

  def G: GenExtractor[FR, Int] = GroupIndexExpr

  def A(selector: String): GenExtractor[FR, Elements[Unstructured]] = 'A.findAll(selector)
  def A(selector: String, i: Int): Extractor[Unstructured] = {
    val expr = 'A.findAll(selector)
    expr.get(i)
  }
}

object DSL extends DSL
