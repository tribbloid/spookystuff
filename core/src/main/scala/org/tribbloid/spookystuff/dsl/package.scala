package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.actions.{Action, Trace, TraceSetView}
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.{Page, Unstructured, Elements}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
* Created by peng on 9/18/14.
*/
package object dsl {

//  type SerializableCookie = Cookie with Serializable

  implicit def traceSetView(traces: Set[Trace]): TraceSetView = new TraceSetView(traces)

  implicit def actionToTraceSet(action: Action): Set[Trace] = Set(Trace(Seq(action)))

  implicit def actionToTraceSetView(action: Action): TraceSetView = Set(Trace(Seq(action)))

  //------------------------------------------------------------

  //this hierarchy aims to create a short DSL for selecting components from PageRow, e.g.:
  //'abc:  cells with key "abc", tempkey precedes ordinary key
  //'abc.$("div#a1"): all children of an unstructured field (either a page or element) that match the selector
  //$("div#a1"): all children of the only page that match the selector, if multiple page per row, throws an exception
  //$_*("div#a1"): all children of all pages that match the selector.
  //'abc.$("div#a1").head: first child of an unstructured field (either a page or element) that match the selector
  //'abc.$("div#a1").text: first text of an unstructured field that match the selector
  //'abc.$("div#a1").texts: all texts of an unstructured field that match the selector
  //'abc.$("div#a1").attr("src"): first "src" attribute of an unstructured field that match the selector
  //'abc.$("div#a1").attrs("src"): first "src" attribute of an unstructured field that match the selector

  def $(selector: String): ChildrenExpr = GetOnlyPageExpr.children(selector)
  def $(selector: String, i: Int): Expression[Unstructured] = GetOnlyPageExpr.children(selector).get(i)
  def $: Expression[Page] = GetOnlyPageExpr

  def $_*(selector: String): ChildrenExpr = GetAllPagesExpr.children(selector)
  def $_*(selector: String, i: Int): Expression[Unstructured] = GetAllPagesExpr.children(selector).get(i)
  def `$_*`: Expression[Elements[Page]] = GetAllPagesExpr

  def A(selector: String): ChildrenExpr = 'A.children(selector)
  def A(selector: String, i: Int): Expression[Unstructured] = 'A.children(selector).get(i)

  implicit def exprView[T: ClassTag](expr: Expression[T]): ExprView[T] =
    new ExprView(expr)

  implicit def unstructuredExprView(expr: Expression[Unstructured]): UnstructuredExprView =
    new UnstructuredExprView(expr)

  implicit def pageExprView(expr: Expression[Page]): PageExprView =
    new PageExprView(expr)

  implicit def elementsExprView(expr: Expression[Elements[_]]): ElementsExprView =
    new ElementsExprView(expr)

  implicit def IterableExprView[T: ClassTag](expr: Expression[Iterable[T]]): IterableExprView[T] =
    new IterableExprView[T](expr)

  implicit def stringExprView(expr: Expression[String]): StringExprView =
    new StringExprView(expr)

  //--------------------------------------------------

  implicit def symbolToExpr(symbol: Symbol): GetExpr =
    new GetExpr(symbol.name)

//  implicit def symbolToStrExpr(symbol: Symbol): Expr[String] = exprView[Any](new GetExpr(symbol.name)).map(_.toString)

  implicit def symbolToExprView(symbol: Symbol): ExprView[Any] =
    new GetExpr(symbol.name)

  implicit def symbolToUnstructuredExprView(symbol: Symbol): UnstructuredExprView =
    new GetUnstructuredExpr(symbol.name)

  implicit def symbolToPageExprView(symbol: Symbol): PageExprView =
    new GetPageExpr(symbol.name)

  implicit def symbolToSeqExprView(symbol: Symbol): IterableExprView[Any] =
    new GetSeqExpr(symbol.name)

  implicit def stringToExpr(str: String): Expression[String] = {

    val delimiter = Const.keyDelimiter
    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    if (regex.findFirstIn(str).isEmpty)
      new Literal[String](str)
    else
      new ReplaceKeyExpr(str)
  }

  implicit class StrContextHelper(val strC: StringContext) {

    def x(fs: Expression[Any]*) = new InterpolateExpr(strC.parts, fs)

    def $() = GetOnlyPageExpr.children(strC.parts.mkString)

    def $_*() = GetAllPagesExpr.children(strC.parts.mkString)

    def A() = 'A.children(strC.parts.mkString)
  }

  //--------------------------------------------------

//  implicit def UnstructuredIterableView(self: Iterable[Unstructured]): UnstructuredGroup =
//    new UnstructuredGroup(self)
}