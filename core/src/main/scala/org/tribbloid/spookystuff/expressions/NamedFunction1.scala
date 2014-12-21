package org.tribbloid.spookystuff.expressions

/**
 * Created by peng on 11/28/14.
 */
trait NamedFunction1[-T, +R] extends (T => R) with Serializable {

  var name: String
  var hasDefaultName: Boolean = true

  final def as(name: Symbol): this.type = {
    assert(name != null)

    this.name = name.name
    this.hasDefaultName = false
    this
  }

  final def defaultAs(name: Symbol): this.type = {
    assert(name != null)

    if (hasDefaultName) this.name = name.name
    this
  }

  final def ~(name: Symbol): this.type = as(name)

  @annotation.unspecialized override def compose[A](g: A => T): NamedFunction1[A, R] =
    NamedFunction1(
      v1 => NamedFunction1.this.apply(g(v1)),
      g.toString() + "." + NamedFunction1.this.name
    )

  @annotation.unspecialized override def andThen[A](g: R => A): NamedFunction1[T, A] =
    NamedFunction1(
      v1 => g(NamedFunction1.this.apply(v1)),
      NamedFunction1.this.name + "." + g.toString()
    )

  final override def toString(): String = name

  //  override def hashCode = this.toString().hashCode
  //
  //  override def equals(any: Any) = this.toString().equals(any)
}

object NamedFunction1 {

  def apply[T, R](f: T => R, _name: String): NamedFunction1[T, R] =
    new NamedFunction1[T, R] {

      override var name = _name

      override def apply(v1: T): R = f(v1)
    }

//  def apply[T, R](f: T => R): NamedFunction1[T, R] = apply(f.toString(), f)
}