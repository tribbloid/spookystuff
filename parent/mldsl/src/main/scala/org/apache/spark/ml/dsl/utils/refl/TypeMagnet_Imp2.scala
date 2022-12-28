package org.apache.spark.ml.dsl.utils.refl

import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class TypeMagnet_Imp2 extends TypeMagnet_Imp1 {

  object FromClassTag extends CachedBuilder[ClassTag] {

    override def createNew[T](v: ClassTag[T]): TypeMagnet[T] = new FromClassTag(v)
  }

  implicit def _fromClassTag[T](v: ClassTag[T]): TypeMagnet[T] = FromClassTag(v)
  implicit def __fromClassTag[T](
      implicit
      v: ClassTag[T]
  ): TypeMagnet[T] = FromClassTag(v)
}
