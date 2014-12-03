package org.tribbloid.spookystuff.expressions

import scala.reflect.ClassTag

/**
 * Created by peng on 10/11/14.
 * This is the preferred way of defining extraction
 * entry point for all "query lambda"
 */

//name: target column
//this hierarchy aims to create a short DSL for selecting components from PageRow, e.g.:
//'abc:  cells with key "abc", tempkey precedes ordinary key
//'abc("div#a1"): all elements of an unstructured field (either a page or element) that match the selector
//'*("div#a1"): all elements of the only page that match the selector, if multiple page per row, throws an exception
//'abc("div#a1").head: first element of an unstructured field (either a page or element) that match the selector
//'abc("div#a1").text: all texts of an unstructured field that match the selector
//'abc("div#a1").attr("src").head: first "src" attribute of an unstructured field that match the selector

class ExprView[T: ClassTag](self: Expr[T]) {

  def map[A](g: T => A): Expr[A] = self.andThen(_.map(v => g(v))) //TODO: this downcast should work

  def map[A](g: T => A, name: String): Expr[A] = map(NamedFunction1(g, name))

  def flatMap[A](g: T => Option[A]): Expr[A] = self.andThen(_.flatMap(v => g(v)))

  def flatMap[A](g: T => Option[A], name: String): Expr[A] = flatMap(NamedFunction1(g, name))

//  def defaultToHrefExpr = (self match {
//    case expr: Expr[Unstructured] => expr.href
//    case expr: Expr[Seq[Unstructured]] => expr.hrefs
//    case _ => self
//  }) > Symbol(Const.joinExprKey)

//  def defaultToTextExpr = (this match {
//    case expr: Expr[Unstructured] => expr.text
//    case expr: Expr[Seq[Unstructured]] => expr.texts
//    case _ => this
//  }) as Symbol(Const.joinExprKey)
}
