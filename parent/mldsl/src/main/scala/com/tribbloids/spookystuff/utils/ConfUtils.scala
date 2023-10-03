package com.tribbloids.spookystuff.utils

import org.apache.spark.{SparkConf, SparkEnv}
import org.slf4j.LoggerFactory

object ConfUtils {

  def getPropertyOrEnv(
      property: String
  )(
      implicit
      sparkConf: SparkConf = {
        Option(SparkEnv.get)
          .map(_.conf)
          .getOrElse(
            new SparkConf()
          )
      }
  ): Option[String] = {

    val env = property.replace('.', '_').toUpperCase

    val result = Option(System.getProperty(property))
      .filter(_.toLowerCase != "null")
      .map { v =>
        LoggerFactory
          .getLogger(this.getClass)
          .info(
            s"System property:\t $property -> $v"
          )
        v
      }
      .orElse {

        Option(System.getenv(env)).filter(_.toLowerCase != "null").map { v =>
          LoggerFactory
            .getLogger(this.getClass)
            .info(
              s"Environment variable:\t $env -> $v"
            )
          v
        }
      }
      .orElse {

        sparkConf
          .getOption(property)
          .map { v =>
            LoggerFactory
              .getLogger(this.getClass)
              .info(
                s"Spark Configurations:\t $property -> $v"
              )
            v
          }
          .filter(_.toLowerCase != "null")
      }

    result
  }

  /**
    * spark config >> system property >> system environment >> default
    */
  def getOrDefault(
      property: String,
      default: String = null
  )(
      implicit
      sparkConf: SparkConf = Option(SparkEnv.get)
        .map(_.conf)
        .getOrElse(
          new SparkConf()
        )
  ): String = {

    val v = getPropertyOrEnv(property)(sparkConf)
    v.getOrElse {
      default
    }
  }

}
