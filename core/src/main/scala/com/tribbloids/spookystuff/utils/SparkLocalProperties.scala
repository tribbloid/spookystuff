package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.SpookyViewsSingleton.{SPARK_JOB_DESCRIPTION, SPARK_JOB_GROUP_ID}
import org.apache.spark.SparkContext

case class SparkLocalProperties(@transient val ctx: SparkContext) {

  val groupID: String = ctx.getLocalProperty(SPARK_JOB_GROUP_ID)
  val description: String = ctx.getLocalProperty(SPARK_JOB_DESCRIPTION)
}
