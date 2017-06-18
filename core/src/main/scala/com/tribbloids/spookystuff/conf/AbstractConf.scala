package com.tribbloids.spookystuff.conf

import org.apache.spark.ml.dsl.utils.MessageAPI
import org.apache.spark.{SparkConf, SparkEnv}
import org.slf4j.LoggerFactory

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

object AbstractConf {

  def getPropertyOrEnv(
                        property: String
                      )(implicit conf: SparkConf = Option(SparkEnv.get).map(_.conf).orNull): Option[String] = {

    val env = property.replace('.','_').toUpperCase

    Option(System.getProperty(property)).filter (_.toLowerCase != "null").map {
      v =>
        LoggerFactory.getLogger(this.getClass).info(s"System has property $property -> $v")
        v
    }
      .orElse {
        Option(System.getenv(env)).filter (_.toLowerCase != "null").map {
          v =>
            LoggerFactory.getLogger(this.getClass).info(s"System has environment $env -> $v")
            v
        }
      }
      .orElse {
        Option(conf) //this is ill-suited for third-party application, still here but has lowest precedence.
          .flatMap(
          _.getOption(property).map {
            v =>
              LoggerFactory.getLogger(this.getClass).info(s"SparkConf has property $property -> $v")
              v
          }
        )
          .filter (_.toLowerCase != "null")
      }
  }

  /**
    * spark config >> system property >> system environment >> default
    */
  def getOrDefault(
                    property: String,
                    default: String = null
                  )(implicit conf: SparkConf = Option(SparkEnv.get).map(_.conf).orNull): String = {

    getPropertyOrEnv(property)
      .getOrElse{
        default
      }
  }
}