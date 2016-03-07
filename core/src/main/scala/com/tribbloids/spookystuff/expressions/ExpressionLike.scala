package com.tribbloids.spookystuff.expressions

import com.tribbloids.spookystuff.row.Field

import scala.language.implicitConversions

/**
 * Created by peng on 11/28/14.
 */
object ExpressionLike {

  def apply[T, R](f: T => R, _field: Field): ExpressionLike[T, R] =
    new ExpressionLike[T, R] {

      override val field = _field

      override def apply(v1: T): R = f(v1)
    }

//  def apply[T, R](f: T => R, name: String): ExpressionLike[T, R] = apply(f, Field(name))

  def apply[T, R](f: T => R): ExpressionLike[T, R] = this.apply(f, Field(""+f.hashCode()))

  implicit def strToField(str: String): Field = Field(str)

  //  def apply[T, R](f: T => R): NamedFunction1[T, R] = apply(f.toString(), f)
}

trait ExpressionLike[-T, +R] extends (T => R) with Serializable {

  val field: Field

  final def name: String = field.name

  //if field==null will revert alias
  final def as(field: Field): ExpressionLike[T, R] = Alias(this, Option(field))
  final def ~(field: Field) = as(field)

  final def as_!(field: Field): ExpressionLike[T, R] = Alias.apply(this, Option(field).map(_.!))
  final def ~!(field: Field) = as_!(field)

  final def as_*(field: Field): ExpressionLike[T, R] = Alias.apply(this, Option(field).map(_.*))
  final def ~*(field: Field) = as_*(field)

  //will not rename an already-named Alias.
  def defaultAs(field: Field): ExpressionLike[T, R] = as(field)

  @annotation.unspecialized override def compose[A](g: A => T): ExpressionLike[A, R] = {
    val gName = g match {
      case ee: ExpressionLike[A, T] => ee.name
      case _ => g.toString()
    }

    ExpressionLike(
      v1 => ExpressionLike.this.apply(g(v1)),
      this.field.copy(name = gName + "." + ExpressionLike.this.name)
    )
  }

  @annotation.unspecialized override def andThen[A](g: R => A): ExpressionLike[T, A] = {
    val gName = g match {
      case ee: ExpressionLike[R, A] => ee.name
      case _ => g.toString()
    }

    ExpressionLike(
      v1 => g(ExpressionLike.this.apply(v1)),
      this.field.copy(name = ExpressionLike.this.name + "." + gName)
    )
  }

  override def toString(): String = name
}

object Alias {

  def apply[T, R](src: ExpressionLike[T, R], fieldOpt: Option[Field]): ExpressionLike[T, R] = {

    val unwrapped: ExpressionLike[T, R] = unwrap(src)

    fieldOpt match {
      case Some(field) => new Alias(unwrapped, field)
      case None => unwrapped
    }
  }

  def unwrap[R, T](src: ExpressionLike[T, R]): ExpressionLike[T, R] = {
    val self: ExpressionLike[T, R] = src match {
      case a: Alias[T, R] => a.self
      case _ => src
    }
    self
  }
}

class Alias[-T, +R](
                             val self: ExpressionLike[T, R],
                             override val field: Field
                           ) extends ExpressionLike[T, R] {

  assert(field != null)

  override def apply(v1: T): R = self(v1)

  override def defaultAs(field: Field): Alias[T, R] = this

  override def toString() = self.toString + " ~ '" + name
}