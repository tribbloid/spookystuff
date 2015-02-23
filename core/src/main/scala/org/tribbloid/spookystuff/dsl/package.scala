package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.actions.{Action, Trace, TraceSetView}
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.{Page, Unstructured, UnstructuredIterableView}

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

  def $(selector: String, i: Int): Expression[Unstructured] = GetOnlyPageExpr.children(selector).get(i)

  def $(selector: String): Expression[Seq[Unstructured]] = GetOnlyPageExpr.children(selector)

  def $: Expression[Page] = GetOnlyPageExpr

  def $_*(selector: String, i: Int): Expression[Unstructured] = GetAllPagesExpr.allChildren(selector).get(i)

  def $_*(selector: String): Expression[Iterable[Unstructured]] = GetAllPagesExpr.allChildren(selector)

  def `$_*`: Expression[Seq[Page]] = GetAllPagesExpr

  def A(selector: String, i: Int): Expression[Unstructured] = 'A.children(selector).get(i)

  def A(selector: String): Expression[Seq[Unstructured]] = 'A.children(selector)

  implicit def exprView[T: ClassTag](expr: Expression[T]): ExprView[T] =
    new ExprView(expr)

  implicit def unstructuredExprView(expr: Expression[Unstructured]): UnstructuedExprView =
    new UnstructuedExprView(expr)

  implicit def pageExprView(expr: Expression[Page]): PageExprView =
    new PageExprView(expr)

  implicit def UnstructuedIterableExprView(expr: Expression[Iterable[Unstructured]]): UnstructuedIterableExprView =
    new UnstructuedIterableExprView(expr)

  implicit def unstructuredIterableExprToUnstructuredExprView(expr: Expression[Iterable[Unstructured]]): UnstructuedExprView =
    expr.head

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

  implicit def symbolToUnstructuredExprView(symbol: Symbol): UnstructuedExprView =
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

    def $_*() = GetAllPagesExpr.allChildren(strC.parts.mkString)

    def A() = 'A.children(strC.parts.mkString)
  }

  //--------------------------------------------------

  implicit def UnstructuredIterableView(self: Iterable[Unstructured]): UnstructuredIterableView =
    new UnstructuredIterableView(self)
}