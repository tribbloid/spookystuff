package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyViewsConst
import org.apache.spark.SparkContext

case class SparkLocalProperties(@transient val ctx: SparkContext) {

  val groupID: String = ctx.getLocalProperty(SpookyViewsConst.SPARK_JOB_GROUP_ID)
  val description: String = ctx.getLocalProperty(SpookyViewsConst.SPARK_JOB_DESCRIPTION)
}
