package org.apache.spark.ml.dsl.utils

trait ObjectSimpleNameMixin {
  // TODO: cleanup, encoding may change in scala 3
  @transient lazy val objectSimpleName: String = ObjectSimpleNameMixin.get(this)
}

object ObjectSimpleNameMixin {

  def get(v: Any): String = {

    if (v == null) "null"
    else {
      // TODO: need to decode to Scala name instead of JVM name
      v.getClass.getSimpleName
        .stripSuffix("$")
        .split('$')
        .filter(_.nonEmpty)
        .head
    }
  }
}
