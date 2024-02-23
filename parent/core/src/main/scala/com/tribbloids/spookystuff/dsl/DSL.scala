package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors._

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

class DSL extends DSL_Imp0 {

  import com.tribbloids.spookystuff.extractors.impl.Extractors._

  def S: GenExtractor[FR, Doc] = GetOnlyDocExpr
  def S(selector: String): GenExtractor[FR, Elements[Unstructured]] = S.andMap(_.root).findAll(selector)
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
