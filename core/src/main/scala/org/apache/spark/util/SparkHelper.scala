package org.apache.spark.util

import org.apache.hadoop.security.UserGroupInformation

/**
  * Created by peng on 17/05/17.
  */
object SparkHelper {

  def serviceUGI: UserGroupInformation = {

    val user = UserGroupInformation.getCurrentUser
    val service = Option(user.getRealUser).getOrElse(user)
    service
  }
}
