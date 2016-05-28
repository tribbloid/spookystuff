package com.tribbloids.spookystuff.extractors

import java.beans.Transient

import com.tribbloids.spookystuff.row.Field
import com.tribbloids.spookystuff.utils.{PrettyToStringMixin, Utils}
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.runtime.AbstractPartialFunction

import ScalaReflection.universe.TypeTag

object GenExtractor {

  implicit def fn2GenExtractor[T, R](self: T => R): GenExtractor[T, R] = {

    self match {
      case e: GenExtractor[T, R] =>
        e
      case pf: PartialFunction[T, R] =>
        new PartialGenExtractor(pf)
      case _ =>
        new PartialGenExtractor(PartialFunction(self))
    }
  }
}

//doesn't carry a classTag, to add any function that use classTag, please add to dsl.ExprView
//TODO: convert to TreeNode?
trait GenExtractor[T, +R] extends PartialFunction[T, R] with PrettyToStringMixin with DynamicHelper[T, R] {

  lazy val unboxed: PartialFunction[T, R] = this

  def _as(fieldOpt: Option[Field]): GenExtractor[T, R] = {

    fieldOpt match {
      case Some(field) => new AliasLike[T, R](unboxed, field)
      case None => unboxed
    }
  }

  final def as(field: Field) = _as(Option(field))
  final def ~(field: Field) = as(field)

  final def as_!(field: Field) = _as(Option(field).map(_.!))
  final def ~!(field: Field) = as_!(field)

  final def as_*(field: Field) = _as(Option(field).map(_.*))
  final def ~*(field: Field) = as_*(field)

  def _named(field: Field): AliasLike[T, R] = new AliasLike[T, R](unboxed, field)

  final def named(field: Field) = _named(field)

  final def named_!(field: Field) = _named(field.!)

  final def named_*(field: Field) = _named(field.*)

  //will not rename an already-named Alias.
  def defaultAs(field: Field): GenExtractor[T, R] = as(field)

  //alas, default impls are not serializable
  override def andThen[A](g: R => A): GenExtractor[T, A] = new AndThenGenExtractor[T, R, A](this, g)

  def andOptional[A](g: R => Option[A]): GenExtractor[T, A] = new AndThenGenExtractor[T, R, A](this, OptionalGenExtractor(g))

  //TODO: extract subroutine and use it to avoid obj creation overhead
  def typed[A](implicit ev: ClassTag[A]): GenExtractor[T, A] = andOptional[A]{
    Utils.typedOrNone[A]
  }
}

trait UnliftedGenExtractor[T, +R] extends AbstractPartialFunction[T, R] with GenExtractor[T, R] {

  def liftApply(v1: T): Option[R]

  override final def isDefinedAt(x: T): Boolean = liftApply(x).isDefined

  override final def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = {
    val z = liftApply(x)
    if (z.isDefined) z.get else default(x)
  }

  override final def lift = liftApply
}

case class PartialGenExtractor[T, +R](delegate: PartialFunction[T, R]) extends GenExtractor[T, R] {

  override lazy val unboxed: PartialFunction[T, R] = delegate

  override final def apply(v1: T): R = delegate(v1)
  override final def isDefinedAt(x: T): Boolean = delegate.isDefinedAt(x)

  override def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = delegate.applyOrElse(x, default)

  override def lift = delegate.lift
}

case class OptionalGenExtractor[T, +R](delegate: T => Option[R]) extends UnliftedGenExtractor[T, R] {

  override def liftApply(v1: T): Option[R] = delegate(v1)
}

case class AndThenGenExtractor[A, B, +C](
                                          pf: GenExtractor[A, B],
                                          g: GenExtractor[B, C]
                                          ) extends UnliftedGenExtractor[A, C] {

  override def liftApply(v1: A): Option[C] = pf.lift.apply(v1).flatMap(g.lift)
}

trait NamedGenExtractor[T, +R] extends GenExtractor[T, R] {
  def field: Field

  assert(field != null)

  def name = field.name

  override def toString = field.name

  override def defaultAs(field: Field) = this
}

class AliasLike[T, +R](
                       override val delegate: PartialFunction[T, R],
                       val field: Field
                     ) extends PartialGenExtractor[T, R](delegate) with NamedGenExtractor[T, R] {

}