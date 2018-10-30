package org.apache.spark.sql.utils

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionsUtils

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

  def internalCreateDF(
      sql: SQLContext,
      rdd: RDD[InternalRow],
      schema: StructType
  ) = {

    sql.internalCreateDataFrame(rdd, schema)
  }

  def _CollectionsUtils: CollectionsUtils.type = CollectionsUtils

  def _RangePartitioner: RangePartitioner.type = RangePartitioner
}
