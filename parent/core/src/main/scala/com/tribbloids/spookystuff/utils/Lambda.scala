package com.tribbloids.spookystuff.utils

import scala.language.implicitConversions
import scala.runtime.ScalaRunTime

/**
  * Create to override the lame scala <functionN> toString format
  */
// TODO: should be a view instead of a subclass
trait Lambda[-T, +R] extends Function1[T, R] with Serializable {
  import Lambda._

  override val toString: String = {
    this match {
      case p: Product =>
        ScalaRunTime._toString(p) // TODO: use tree string
      case _ =>
        getClass.getSimpleName + ": " + super.toString // TODO: this gives very little information, should emit code position
    }
  }

  override def andThen[A](g: R => A): AndThen[T, R, A] = {
    AndThen(this, (g: Lambda[R, A]).asInstanceOf[Lambda[Any, A]])
  }
}

object Lambda {

  case class AndThen[-A, +R, +C](
      ab: Lambda[A, R],
      bc: Lambda[Any, C]
  ) extends Lambda[A, C] {
    override def apply(v: A): C = {
      bc.apply(ab(v))
    }
  }

  implicit def box1[T, R](fn: T => R): Lambda[T, R] = {
    fn match {
      case _: Lambda[_, _] => fn.asInstanceOf[Lambda[T, R]]
      case _               => v => fn(v)
    }
  }

  implicit def box2[T1, T2, R](f: (T1, T2) => R): Lambda[(T1, T2), R] =
    (t: (T1, T2)) => f(t._1, t._2)
}
