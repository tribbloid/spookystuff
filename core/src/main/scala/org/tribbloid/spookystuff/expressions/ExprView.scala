package org.tribbloid.spookystuff.expressions

import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.utils.Default

import scala.reflect._

/**
 * Created by peng on 10/11/14.
 * This is the preferred way of defining extraction
 * entry point for all "query lambda"
 */

final class ExprView[+T: ClassTag](self: Expression[T]) {

  def defaultVal: T = Default.value[T]

  def andMap[A](g: T => A): Expression[A] = self.andThen(_.map(v => g(v)))

  def andMap[A](g: T => A, name: String): Expression[A] = self.andThen(NamedFunction1(_.map(v => g(v)), name))

  def andFlatMap[A](g: T => Option[A]): Expression[A] = self.andThen(_.flatMap(v => g(v)))

  def andFlatMap[A](g: T => Option[A], name: String): Expression[A] = self.andThen(NamedFunction1(_.flatMap(v => g(v)), name))

  def typed[A](implicit ev: ClassTag[A]) = this.andFlatMap[A](
    {
      case res: A => Some(res)
      case _ => None
    }: T => Option[A],
    s"filterByType[${ev.toString()}}]"
  )

  def into(name: Symbol): Expression[Traversable[T]] = new InsertIntoExpr[T](name.name, self)
  def ~+(name: Symbol) = into(name)

  //these will convert Expression to a common function
  def getOrElse[B >: T](value: =>B = defaultVal): NamedFunction1[PageRow, B] = self.andThen(
    NamedFunction1(_.getOrElse(value), s"getOrElse($value)")
  )

  def get: NamedFunction1[PageRow, T] = self.andThen(
    NamedFunction1(_.get, s"get")
  )

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
