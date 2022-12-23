package org.apache.spark.ml.dsl.utils

trait ObjectSimpleNameMixin {

  @transient lazy val objectSimpleName: String = ObjectSimpleNameMixin.get(this)
}

object ObjectSimpleNameMixin {

  def get(v: Any): String = {

    if (v == null) "null"
    else {
      v.getClass.getSimpleName
        .stripSuffix("$")
        .split('$')
        .filter(_.nonEmpty)
        .head
    }
  }
}
