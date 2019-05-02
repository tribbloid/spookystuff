package com.tribbloids.spookystuff.utils

import org.apache.spark.{SparkConf, SparkEnv}
import org.slf4j.LoggerFactory

object ConfUtils {

  def getPropertyOrEnv(
      property: String
  )(implicit conf: SparkConf = Option(SparkEnv.get).map(_.conf).orNull): Option[String] = {

    val env = property.replace('.', '_').toUpperCase

    val result = Option(System.getProperty(property))
      .filter(_.toLowerCase != "null")
      .map { v =>
        LoggerFactory.getLogger(this.getClass).info(s"System has property $property -> $v")
        v
      }
      .orElse {
        Option(System.getenv(env)).filter(_.toLowerCase != "null").map { v =>
          LoggerFactory.getLogger(this.getClass).info(s"System has environment $env -> $v")
          v
        }
      }
      .orElse {
        Option(conf) //this is ill-suited for third-party application, still here but has lowest precedence.
          .flatMap(
            _.getOption(property).map { v =>
              LoggerFactory.getLogger(this.getClass).info(s"SparkConf has property $property -> $v")
              v
            }
          )
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
  )(implicit conf: SparkConf = Option(SparkEnv.get).map(_.conf).orNull): String = {

    val v = getPropertyOrEnv(property)
    v.getOrElse {
      default
    }
  }

  /**
    * change UnmodifiableMap System.getenv() for tests
    * NOT stable! Only for testing
    */
  def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }
}
