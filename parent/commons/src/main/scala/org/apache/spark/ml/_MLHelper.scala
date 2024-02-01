package org.apache.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter}
import org.json4s.{JObject, JValue}

/**
  * Created by peng on 17/05/17.
  */
object _MLHelper {

  //  def serviceUGI: UserGroupInformation = {
  //
  //    val user = UserGroupInformation.getCurrentUser
  //    val service = Option(user.getRealUser).getOrElse(user)
  //    service
  //  }

  object _DefaultParamsReader {

    val loadMetadata: (String, SparkContext, String) => DefaultParamsReader.Metadata = {
      DefaultParamsReader.loadMetadata
    }
  }

  object _DefaultParamsWriter {

    val saveMetadata: (Params, String, SparkContext, Option[JObject], Option[JValue]) => Unit =
      DefaultParamsWriter.saveMetadata
  }
}
