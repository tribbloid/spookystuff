package com.tribbloids.spookystuff.expressions

import com.tribbloids.spookystuff.row.Field
import com.tribbloids.spookystuff.utils.{PrettyToStringMixin, Utils}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.runtime.AbstractPartialFunction

object ExpressionLike {

  implicit def fn2GenExpression[T, R](self: T => R): ExpressionLike[T, R] = {
    self match {
      case e: ExpressionLike[T, R] =>
        e
      case pf: PartialFunction[T, R] =>
        new PartialExpressionLike(pf)
      case _ =>
        new PartialExpressionLike(PartialFunction(self))
    }
  }
}

//doesn't carry a classTag, to add any function that use classTag, please add to dsl.ExprView
trait ExpressionLike[T, +R] extends PartialFunction[T, R] with PrettyToStringMixin with DynamicExpressionMixin[T, R] {

  lazy val unboxed: PartialFunction[T, R] = this

  def _as(fieldOpt: Option[Field]): ExpressionLike[T, R] = {

    fieldOpt match {
      case Some(field) => new AliasLike[T, R](unboxed, field)
      case None => unboxed
    }
  }

  final def as(field: Field): ExpressionLike[T, R] = _as(Option(field))
  final def ~(field: Field) = as(field)

  final def as_!(field: Field): ExpressionLike[T, R] = _as(Option(field).map(_.!))
  final def ~!(field: Field) = as_!(field)

  final def as_*(field: Field): ExpressionLike[T, R] = _as(Option(field).map(_.*))
  final def ~*(field: Field) = as_*(field)

  //will not rename an already-named Alias.
  def defaultAs(field: Field): ExpressionLike[T, R] = as(field)

  //alas, default impls are not serializable
  override def andThen[A](g: R => A): ExpressionLike[T, A] = new AndThenExpressionLike[T, R, A](this, g)

  def andOptional[A](g: R => Option[A]): ExpressionLike[T, A] = new AndThenExpressionLike[T, R, A](this, OptionalExpressionLike(g))

  //TODO: extract subroutine and use it to avoid obj creation overhead
  def typed[A](implicit ev: ClassTag[A]): ExpressionLike[T, A] = andOptional[A]{
    Utils.typedOrNone[A]
  }
}

trait UnliftedExpressionLike[T, +R] extends AbstractPartialFunction[T, R] with ExpressionLike[T, R] {

  def liftApply(v1: T): Option[R]

  override final def isDefinedAt(x: T): Boolean = liftApply(x).isDefined

  override final def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = {
    val z = liftApply(x)
    if (z.isDefined) z.get else default(x)
  }

  override final def lift = liftApply
}

case class PartialExpressionLike[T, +R](delegate: PartialFunction[T, R]) extends ExpressionLike[T, R] {

  override lazy val unboxed: PartialFunction[T, R] = delegate

  override final def apply(v1: T): R = delegate(v1)
  override final def isDefinedAt(x: T): Boolean = delegate.isDefinedAt(x)

  override def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = delegate.applyOrElse(x, default)

  override def lift = delegate.lift
}

case class OptionalExpressionLike[T, +R](delegate: T => Option[R]) extends UnliftedExpressionLike[T, R] {

  override def liftApply(v1: T): Option[R] = delegate(v1)
}

case class AndThenExpressionLike[A, B, +C](
                                            pf: ExpressionLike[A, B],
                                            g: ExpressionLike[B, C]
                                          ) extends UnliftedExpressionLike[A, C] {

  override def liftApply(v1: A): Option[C] = pf.lift.apply(v1).flatMap(g.lift)
}

trait NamedExpressionLike[T, +R] extends ExpressionLike[T, R] {
  def field: Field

  assert(field != null)

  def name = field.name

  override def toString = field.name

  override def defaultAs(field: Field) = this
}

class AliasLike[T, +R](
                       override val delegate: PartialFunction[T, R],
                       val field: Field
                     ) extends PartialExpressionLike[T, R](delegate) with NamedExpressionLike[T, R] {

}