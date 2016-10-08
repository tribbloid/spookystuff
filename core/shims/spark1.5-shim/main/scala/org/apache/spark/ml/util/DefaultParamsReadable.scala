package org.apache.spark.ml.util

import org.apache.spark.ml.dsl.utils.UnsupportedBySpark

/**
  * Created by peng on 05/10/16.
  */
trait DefaultParamsReadable[T] {

  def load(path: String): T = throw UnsupportedBySpark.error
}
