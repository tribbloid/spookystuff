package org.apache.spark.util

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.deploy.SparkHadoopUtil

/**
  * Created by peng on 17/05/17.
  */
object SparkHelper {

  def getSparkUGI: UserGroupInformation = {

    val user = Utils.getCurrentUserName()
    val ugi = UserGroupInformation.createRemoteUser(user)
    SparkHadoopUtil.get.transferCredentials(UserGroupInformation.getCurrentUser, ugi)
    ugi
  }
}
