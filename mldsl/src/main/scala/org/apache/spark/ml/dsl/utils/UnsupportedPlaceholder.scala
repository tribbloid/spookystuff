package org.apache.spark.ml.dsl.utils

import scala.language.dynamics

/**
  * Created by peng on 05/10/16.
  */

trait UnsupportedPlaceholder {

  def detail: String

  lazy val error = new UnsupportedOperationException(
    s"this function is not supported by $detail"
  )

//  def selectDynamic(name: String): Null = throw error
//
//  def applyDynamic(methodName: String)(args: Any*): Null = throw error
}

class UnsupportedUntilSpark(featureVersion: String = null) extends UnsupportedPlaceholder {

  override def detail = s"Apache Spark ${org.apache.spark.SPARK_VERSION}, please upgrade to " +
    Option(featureVersion)
      .map {
        v =>
          s">= v$v"
      }
      .getOrElse("a later version")
}

object UnsupportedBySpark extends UnsupportedUntilSpark("1.6")