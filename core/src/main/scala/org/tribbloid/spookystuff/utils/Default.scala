package org.tribbloid.spookystuff.utils

import scala.collection.immutable

class Default[+A](val default: A)

trait LowerPriorityImplicits {
  // Stop AnyRefs from clashing with AnyVals
  implicit def defaultNull[A <: AnyRef]:Default[A] = new Default[A](null.asInstanceOf[A])
}

object Default extends LowerPriorityImplicits {
  implicit object DefaultDouble extends Default[Double](0.0)
  implicit object DefaultFloat extends Default[Float](0.0F)
  implicit object DefaultInt extends Default[Int](0)
  implicit object DefaultLong extends Default[Long](0L)
  implicit object DefaultShort extends Default[Short](0)
  implicit object DefaultByte extends Default[Byte](0)
  implicit object DefaultChar extends Default[Char]('\u0000')
  implicit object DefaultBoolean extends Default[Boolean](false)
  implicit object DefaultUnit extends Default[Unit](())

  implicit def defaultSeq[A]: Default[immutable.Seq[A]] = new Default[immutable.Seq[A]](Nil)
  implicit def defaultSet[A]: Default[Set[A]] = new Default[Set[A]](Set())
  implicit def defaultMap[A, B]: Default[Map[A, B]] = new Default[Map[A, B]](Map[A, B]())
  implicit def defaultOption[A]: Default[Option[A]] = new Default[Option[A]](None)

  def value[A](implicit value: Default[A]): A = value.default
}