package com.tribbloids.spookystuff.commons.refl

import scala.language.implicitConversions

trait ClassOpsMixin {}

object ClassOpsMixin {

  implicit class _ops[T](_self: Class[T]) {

    lazy val simpleName_Scala: String = {

      // TODO: need to decode to Scala name instead of JVM name
      _self.getSimpleName
        .stripSuffix("$")
        .split('$')
        .filter(_.nonEmpty)
        .head
    }
  }
}
