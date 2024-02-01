package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CollectionsUtils
import org.apache.spark.{RangePartitioner, SparkContext, SparkEnv}

import scala.reflect.ClassTag

/**
  * Created by peng on 17/05/17.
  */
object _SQLHelper {

  def internalCreateDF(
      sql: SQLContext,
      rdd: RDD[InternalRow],
      schema: StructType
  ): DataFrame = {

    sql.internalCreateDataFrame(rdd, schema)
  }

  def rddClassTag[T](rdd: RDD[T]): ClassTag[T] = rdd.elementClassTag

  def _CollectionsUtils: CollectionsUtils.type = CollectionsUtils

  def _RangePartitioner: RangePartitioner.type = RangePartitioner

  def withScope[T](sc: SparkContext)(fn: => T): T = {
    sc.withScope(fn)
  }

  def blockManagerIDOpt: Option[BlockManagerId] = {
    Option(SparkEnv.get).map(v => v.blockManager.blockManagerId)
  }

  def taskLocationOpt: Option[TaskLocation] = {

    blockManagerIDOpt.map { bmID =>
      TaskLocation(bmID.host, bmID.executorId)
    }
  }

  /**
    * From doc of org.apache.spark.scheduler.TaskLocation Create a TaskLocation from a string returned by
    * getPreferredLocations. These strings have the form executor_[hostname]_[executorid], [hostname], or
    * hdfs_cache_[hostname], depending on whether the location is cached. def apply(str: String): TaskLocation ... Not
    * sure if it will change in future Spark releases
    */
  def taskLocationStrOpt: Option[String] = {

    taskLocationOpt.map {
      _.toString
    }
  }
}
