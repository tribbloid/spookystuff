package com.tribbloids.spookystuff.commons.refl

import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class FromClassTagMixin extends FromClassMixin {

  implicit def _fromClassTag[T](v: ClassTag[T]): TypeMagnet[T] = FromClassTag(v)
  implicit def __fromClassTag[T](
      implicit
      v: ClassTag[T]
  ): TypeMagnet[T] = FromClassTag(v)
}
