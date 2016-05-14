package com.tribbloids.spookystuff.expressions

import com.tribbloids.spookystuff.row.Field
import com.tribbloids.spookystuff.utils.Utils

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.runtime.AbstractPartialFunction

object ExpressionLike {

  implicit def fn2GenExpression[T, R](self: T => R): ExpressionLike[T, R] = {
    self match {
      case pf: PartialFunction[T, R] =>
        new UnaryExpressionLike(pf)
      case _ =>
        new UnaryExpressionLike(PartialFunction(self))
    }
  }

  def apply[T, R](self: T => R, fieldOpt: Option[Field]): ExpressionLike[T, R] = {

    fn2GenExpression(self)._as(fieldOpt)
  }
}

trait ExpressionLike[T, +R] extends PartialFunction[T, R] with Serializable with DynamicExpressionMixin[T, R] {

  def name: String = this.toString()

  lazy val unboxed: PartialFunction[T, R] = this

  //TODO: add type to field
  def _as(fieldOpt: Option[Field]): ExpressionLike[T, R] = {

    fieldOpt match {
      case Some(field) => new GenAlias[T, R](unboxed, field)
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
  override def andThen[A](g: R => A): ExpressionLike[T, A] = new AndThenExpressionLike[T, R, A](this.unboxed, PartialFunction(g))

  def andOptional[A](g: R => Option[A]): ExpressionLike[T, A] = new UnliftExpressionLike[T, A] {

    override def liftApply(v1: T): Option[A] = ExpressionLike.this.lift(v1).flatMap(g)
  }

  //TODO: extract subroutine and use it to avoid obj creation overhead
  def typed[A](implicit ev: ClassTag[A]) = andOptional[A]{
    Utils.typedOrNone[A]
  }
}

class UnaryExpressionLike[T, +R](val child: PartialFunction[T, R]) extends ExpressionLike[T, R] {

  override lazy val unboxed: PartialFunction[T, R] = child

  override final def apply(v1: T): R = child(v1)
  override final def isDefinedAt(x: T): Boolean = child.isDefinedAt(x)

  override def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = child.applyOrElse(x, default)

  override def lift = child.lift
}

trait UnliftExpressionLike[T, +R] extends AbstractPartialFunction[T, R] with ExpressionLike[T, R] {

  def liftApply(v1: T): Option[R]

  override final def isDefinedAt(x: T): Boolean = liftApply(x).isDefined

  override final def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = {
    val z = liftApply(x)
    if (z.isDefined) z.get else default(x)
  }

  override final def lift = liftApply
}

class AndThenExpressionLike[A, B, +C](
                                       val pf: PartialFunction[A, B],
                                       val k: PartialFunction[B, C]
                                     ) extends UnliftExpressionLike[A, C] {

  override def liftApply(v1: A): Option[C] = pf.lift.apply(v1).flatMap(k.lift)
}

trait NamedExpressionLike[T, +R] extends ExpressionLike[T, R] {
  def field: Field

  assert(field != null)

  override def name = field.name

  override def defaultAs(field: Field) = this
}

class GenAlias[T, +R](
                       override val child: PartialFunction[T, R],
                       val field: Field
                      ) extends UnaryExpressionLike[T, R](child) with NamedExpressionLike[T, R] {

}