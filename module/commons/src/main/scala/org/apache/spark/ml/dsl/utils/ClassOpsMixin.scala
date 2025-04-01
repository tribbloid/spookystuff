package org.apache.spark.ml.dsl.utils

import scala.language.implicitConversions

trait ClassOpsMixin {}

object ClassOpsMixin {

  implicit def toClassOps[T](self: Class[T]): ClassOps[T] = {
    // it will be automatically included in scope when working on Class[_ <: ClassOpsMixin]
    // see __ImplicitSearchOrder

    ClassOpsMixin.ClassOps(self)
  }

  case class ClassOps[T](self: Class[T]) {

    lazy val simpleName_Scala: String = {

      // TODO: need to decode to Scala name instead of JVM name
      self.getSimpleName
        .stripSuffix("$")
        .split('$')
        .filter(_.nonEmpty)
        .head

    }
  }

  object ClassOps {
    implicit def unbox[T](self: ClassOps[T]): Unit = self.self
  }
}
