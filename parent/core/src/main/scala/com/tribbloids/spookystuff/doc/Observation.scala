package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.{Export, Trace}
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.io.ResourceMetadata
import org.apache.spark.sql.types.SQLUserDefinedType

import java.sql.{Date, Time, Timestamp}

object Observation {

  trait Success extends Observation

  trait Failure extends Observation

  /**
    * Created by peng on 04/06/14.
    */
  // use to genterate a lookup key for each observation
  @SerialVersionUID(612503421395L)
  case class DocUID(
      backtrace: Trace,
      output: Export,
      //                    sessionStartTime: Long,
      blockIndex: Int = 0,
      blockSize: Int = 1
  )( // number of pages in a block output,
      val name: String = Option(output).map(_.name).orNull
  ) {
    // TODO: name should just be "UID"
  }

}

// all subclasses should be small, will be shipped around by Spark
@SQLUserDefinedType(udt = classOf[FetchedUDT])
sealed trait Observation extends Serializable {

  import Observation.*

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

  def laterThan(v2: Observation): Boolean = this.timeMillis > v2.timeMillis

  def laterOf(v2: Observation): Observation =
    if (laterThan(v2)) this
    else v2

  type RootType
  def root: RootType
  def metadata: ResourceMetadata

  def docForAuditing: Option[Doc]
}
