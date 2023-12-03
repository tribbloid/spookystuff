package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.utils.io.ResourceMetadata
import org.apache.spark.sql.types.SQLUserDefinedType

import java.sql.{Date, Time, Timestamp}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object Fetched {

  trait Success extends Fetched

  trait Failure extends Fetched

  implicit class Observations(
      val vs: Seq[Fetched]
  ) {

    // make sure no pages with identical name can appear in the same group.
    lazy val splitByDistinctNames: Array[Seq[Fetched]] = {
      val outerBuffer: ArrayBuffer[Seq[Fetched]] = ArrayBuffer()
      val buffer: ArrayBuffer[Fetched] = ArrayBuffer()

      vs.foreach { page =>
        if (buffer.exists(_.name == page.name)) {
          outerBuffer += buffer.toList
          buffer.clear()
        }
        buffer += page
      }
      outerBuffer += buffer.toList // always left, have at least 1 member
      buffer.clear()
      outerBuffer.toArray
    }
  }

  object Observations {

    implicit def unbox(v: Observations): Seq[Fetched] = v.vs
  }

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
