package com.tribbloids.spookystuff.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.utils.SparkHelper

import scala.reflect.ClassTag

trait RDDViewBase[T] {

  val self: RDD[T]

  implicit lazy val rddClassTag: ClassTag[T] = SparkHelper.rddClassTag(self)
}
