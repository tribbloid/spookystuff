package com.tribbloids.spookystuff.conf

import org.apache.spark.SparkConf
import org.apache.spark.ml.dsl.utils.MessageAPI

/**
  * all subclasses have to define default() in their respective companion object.
  */
trait AbstractConf extends MessageAPI {

  //  val submodules: Submodules[AbstractConf] = Submodules()

  // TODO: use reflection to automate
  def importFrom(sparkConf: SparkConf): this.type

  //  @transient @volatile var sparkConf: SparkConf = _
  //  def effective: this.type = Option(sparkConf).map {
  //    conf =>
  //      val result = importFrom(conf)
  //      result.sparkConf = this.sparkConf
  //      result
  //  }
  //    .getOrElse(this)
  //    .asInstanceOf[this.type]

  override def clone: this.type = importFrom(new SparkConf())
}