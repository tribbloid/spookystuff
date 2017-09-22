package com.tribbloids.spookystuff.utils

import java.io.{PrintWriter, StringWriter}

import org.apache.hadoop.security.UserGroupInformation

/**
  * Created by peng on 17/05/17.
  */
object SparkHelper {

//  def serviceUGI: UserGroupInformation = {
//
//    val user = UserGroupInformation.getCurrentUser
//    val service = Option(user.getRealUser).getOrElse(user)
//    service
//  }

  // copied from org.apache.spark.util.Utils
  def exceptionString(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }
}
