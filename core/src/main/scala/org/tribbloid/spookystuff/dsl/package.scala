package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.actions.{Action, Trace, TraceSetView}
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.{Page, Unstructured, UnstructuredSeqView}

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

//  val $ = GetPagedExpr("$")
//
//  val A = GetExpr("A")

  def $(selector: String, i: Int): Expr[Unstructured] = Symbol(Const.pageWildCardKey).children(selector).get(i)

  def $(selector: String): Expr[Seq[Unstructured]] = Symbol(Const.pageWildCardKey).children(selector)

  def A(selector: String, i: Int): Expr[Unstructured] = 'A.children(selector).get(i)

  def A(selector: String): Expr[Seq[Unstructured]] = 'A.children(selector)

//  implicit def functionToExpr[R](f: PageRow => Option[R]): Expr[R] = Expr(f)

  implicit def exprView[T: ClassTag](expr: Expr[T]): ExprView[T] =
    new ExprView(expr)

  implicit def unstructuredExprView(expr: Expr[Unstructured]): UnstructuedExprView =
    new UnstructuedExprView(expr)

  implicit def pageExprView(expr: Expr[Page]): PageExprView =
    new PageExprView(expr)

  implicit def unstructuredSeqExprView(expr: Expr[Seq[Unstructured]]): UnstructuedSeqExprView =
    new UnstructuedSeqExprView(expr)

  implicit def unstructuredSeqExprToUnstructuredExprView(expr: Expr[Seq[Unstructured]]): UnstructuedExprView =
    expr.head

  implicit def seqExprView[T: ClassTag](expr: Expr[Seq[T]]): SeqExprView[T] =
    new SeqExprView[T](expr)

  implicit def stringExprView(expr: Expr[String]): StringExprView =
    new StringExprView(expr)

  //--------------------------------------------------

  implicit def symbolToExpr(symbol: Symbol): GetExpr =
    new GetExpr(symbol.name)

//  implicit def symbolToStrExpr(symbol: Symbol): Expr[String] = exprView[Any](new GetExpr(symbol.name)).map(_.toString) //TODO: handle cases where symbol is unstructured

  implicit def symbolToUnstructuredExprView(symbol: Symbol): UnstructuedExprView =
    new GetUnstructuredExpr(symbol.name)

  implicit def symbolToPageExprView(symbol: Symbol): PageExprView =
    new GetPageExpr(symbol.name)

  implicit def stringToExpr(str: String): Expr[String] = {

    val delimiter = Const.keyDelimiter
    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    if (regex.findFirstIn(str).isEmpty)
      new Value[String](str)
    else
      new ReplaceKeyExpr(str)
  }

  implicit class StrContextHelper(val strC: StringContext) {

    def x(fs: Expr[Any]*) = new InterpolateExpr(strC.parts, fs)

    def $() = Symbol(Const.pageWildCardKey).children(strC.parts.mkString)

    def A() = 'A.children(strC.parts.mkString)
    //    def *() = new GetUnstructuredExpr("*").select(strC.parts.head)
  }

  //--------------------------------------------------

  implicit def unstructuredSeqView(self: Seq[Unstructured]): UnstructuredSeqView =
    new UnstructuredSeqView(self)

//  implicit def namedFunction1[T, R](f: T => R): NamedFunction1[T, R] = new NamedFunction1[T, R] {
//
//    override var name = f.toString()
//
//    override def apply(v1: T): R = f(v1)
//  }
}