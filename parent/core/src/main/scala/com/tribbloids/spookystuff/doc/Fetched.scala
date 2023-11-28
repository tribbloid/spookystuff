package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.utils.io.ResourceMetadata
import org.apache.spark.sql.types.SQLUserDefinedType

import java.sql.{Date, Time, Timestamp}

object Fetched {

  trait Success extends Fetched

  trait Failure extends Fetched
}

//keep small, will be passed around by Spark
//TODO: subclass Unstructured to save Message definition
@SQLUserDefinedType(udt = classOf[FetchedUDT])
trait Fetched extends Serializable {

  def uid: DocUID
  def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): this.type

  def cacheLevel: DocCacheLevel.Value

  def name: String = this.uid.name

  def timeMillis: Long

  lazy val date: Date = new Date(timeMillis)
  lazy val time: Time = new Time(timeMillis)
  lazy val timestamp: Timestamp = new Timestamp(timeMillis)

  def laterThan(v2: Fetched): Boolean = this.timeMillis > v2.timeMillis

  def laterOf(v2: Fetched): Fetched =
    if (laterThan(v2)) this
    else v2

  type RootType
  def root: RootType
  def metadata: ResourceMetadata
}
