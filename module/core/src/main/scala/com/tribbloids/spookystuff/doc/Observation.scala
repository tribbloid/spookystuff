package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.SpookyException
import com.tribbloids.spookystuff.actions.{Export, Trace}
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.io.ResourceMetadata
import com.tribbloids.spookystuff.row.AgentContext
import org.apache.spark.sql.types.SQLUserDefinedType

import java.sql.{Date, Time, Timestamp}

object Observation {

  trait Success extends Observation

  trait Error extends SpookyException with Observation {}

  /**
    * Created by peng on 04/06/14.
    */
  // use to genterate a lookup key for each observation
  @SerialVersionUID(612503421395L)
  case class DocUID(
      backtrace: Trace,
      //                    sessionStartTime: Long,
      blockIndex: Int = 0, // TODO: remove, useless
      blockSize: Int = 1
  )( // number of pages in a block output,
      val nameOvrd: Option[String] = None // `export`.name
  ) {

    def `export`: Export = backtrace.lastOption
      .collect {
        case v: Export => v
      }
      .getOrElse {
        val info: String = backtrace.toString()
        throw new UnsupportedOperationException(
          s"ill-formed backtrace, last element should always be an Export: \n ${info}"
        )
      }
    // TODO: this should be a compile-time theorem

    def name: String = nameOvrd.getOrElse(`export`.name)
  }

  trait AgenticMixin {
    self: Observation =>

    def agentState: AgentContext
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
  ): Observation

  def cacheLevel: DocCacheLevel.Value

  def name: String = this.uid.name

  def timeMillis: Long

  lazy val date: Date = new Date(timeMillis)
  lazy val time: Time = new Time(timeMillis)
  lazy val timestamp: Timestamp = new Timestamp(timeMillis)

  final def isLaterThan(v2: Observation): Boolean = this.timeMillis > v2.timeMillis

  final def laterOf(v2: Observation): Observation =
    if (isLaterThan(v2)) this
    else v2

  type RootType
  def root: RootType
  def metadata: ResourceMetadata

  def docForAuditing: Option[Doc]
}
