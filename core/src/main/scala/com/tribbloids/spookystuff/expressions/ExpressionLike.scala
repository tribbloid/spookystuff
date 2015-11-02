package com.tribbloids.spookystuff.expressions

/**
 * Created by peng on 11/28/14.
 */
object ExpressionLike {

  def apply[T, R](f: T => R, _name: String): ExpressionLike[T, R] =
    new ExpressionLike[T, R] {

      override val name = _name

      override def apply(v1: T): R = f(v1)
    }

  def apply[T, R](f: T => R): ExpressionLike[T, R] = this.apply(f, ""+f.hashCode())

  //  def apply[T, R](f: T => R): NamedFunction1[T, R] = apply(f.toString(), f)
}

trait ExpressionLike[-T, +R] extends (T => R) with Serializable {

  val name: String

  final def as(name: Symbol): Alias[T, R] = Alias(this, Option(name).map(_.name).orNull)

  final def as_!(name: Symbol): Alias[T, R] = Alias.forced(this, Option(name).map(_.name).orNull)

  //will not rename an already-named Alias.
  def defaultAs(name: Symbol): Alias[T, R] = as(name)

  final def ~(name: Symbol) = as(name)

  final def ~!(name: Symbol) = as_!(name)

  @annotation.unspecialized override def compose[A](g: A => T): ExpressionLike[A, R] = {
    val gName = g match {
      case ee: ExpressionLike[A, T] => ee.name
      case _ => g.toString()
    }

    ExpressionLike(
      v1 => ExpressionLike.this.apply(g(v1)),
      gName + "." + ExpressionLike.this.name
    )
  }


  @annotation.unspecialized override def andThen[A](g: R => A): ExpressionLike[T, A] = {
    val gName = g match {
      case ee: ExpressionLike[R, A] => ee.name
      case _ => g.toString()
    }

    ExpressionLike(
      v1 => g(ExpressionLike.this.apply(v1)),
      ExpressionLike.this.name + "." + gName
    )
  }

  override def toString(): String = name

  //TODO: experimental, not sure if its in conflict of implicit conversion.
//  def applyDynamic(methodName: String)(args: Any*): ExpressionLike[T, Any] = {
//
//    val resultFn: (T => Any) = {
//      v1 =>
//        val selfValue = this.apply(v1)
//        val argValues: Seq[Any] = args.map {
//          case ee: Function[T,_] => ee.apply(v1)
//          case arg @ _ => arg
//        }
//        val argClasses = argValues.map(_.getClass)
//        val selfClass = selfValue.getClass
//
//        val func = selfClass.getMethod(methodName, argClasses: _*)
//
////        func.getReturnType
//        func.invoke(selfValue, argValues.map(_.asInstanceOf[Object]): _*)
//    }
//
//    val argNames = args map {
//      case ee: ExpressionLike[T, _] => ee.name
//      case arg @ _ => "" + arg
//    }
//
//    val argStr = argNames.mkString(",")
//    val resultName = ExpressionLike.this.name + "." + methodName + "(" + argStr + ")"
//
//    ExpressionLike(resultFn, resultName)
//  }
}

object Alias {

  def apply[T, R](src: ExpressionLike[T, R], name: String): Alias[T, R] = {

    val self: ExpressionLike[T, R] = getSelf(src)

    new Alias(self, name)
  }

  def getSelf[R, T](src: ExpressionLike[T, R]): ExpressionLike[T, R] = {
    val self: ExpressionLike[T, R] = src match {
      case a: Alias[T, R] => a.self
      case _ => src
    }
    self
  }

  def forced[T, R](src: ExpressionLike[T, R], name: String): Alias[T, R] = {

    val self: ExpressionLike[T, R] = getSelf(src)

    new Alias(self, name) with ForceExpressionLike[T, R]
  }
}

class Alias[-T, +R] private(val self: ExpressionLike[T, R], override val name: String) extends ExpressionLike[T, R] {

  override def apply(v1: T): R = self(v1)

  override def defaultAs(name: Symbol): Alias[T, R] = this

  override def toString() = self.toString + " ~ '" + name
}

//subclasses bypass "already exist" check
trait ForceExpressionLike[-T, +R] extends ExpressionLike[T, R]