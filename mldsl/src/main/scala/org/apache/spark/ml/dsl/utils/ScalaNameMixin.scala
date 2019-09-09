package org.apache.spark.ml.dsl.utils

trait ScalaNameMixin {

  @transient lazy val objectSimpleName: String = {
    this.getClass.getSimpleName
      .stripSuffix("$")
      .split('$')
      .filter(_.nonEmpty)
      .head
  }
}
