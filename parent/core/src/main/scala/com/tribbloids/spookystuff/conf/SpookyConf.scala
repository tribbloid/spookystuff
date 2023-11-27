package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.row.Sampler
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.utils.Timeout
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import java.util.Date
import scala.concurrent.duration.Duration.Infinite
import scala.concurrent.duration._

object SpookyConf {

  // DO NOT change to val! all confs are
  // mutable
  def default = new SpookyConf()

  val defaultHTTPHeaders: Map[String, String] = Map(
    "User-Agent" ->
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36"
  )
}

/**
  * Created by peng on 12/06/14. will be shipped to workers
  */
case class SpookyConf(
    var shareMetrics: Boolean = false, // TODO: not necessary

    var webProxy: WebProxyFactory = WebProxyFactories.NoProxy,
    var httpHeadersFactory: () => Map[String, String] = () => SpookyConf.defaultHTTPHeaders,
    var oAuthKeysFactory: () => OAuthKeys = () => null,
    var browserResolution: (Int, Int) = (1920, 1080),
    var remote: Boolean = true, // if disabled won't use remote client at all
    var autoSave: Boolean = true,
    var cacheWrite: Boolean = true,
    var cacheRead: Boolean = true, // TODO: this enable both in-memory and DFS cache, should allow more refined control

    var cachedDocsLifeSpan: Duration = 7.day,
    var IgnoreCachedDocsBefore: Option[Date] = None,
    var cacheFilePath: ByTrace[String] = FilePaths.Hierarchical,
    var autoSaveFilePath: ByDoc[String] = FilePaths.UUIDName(FilePaths.Hierarchical),
    var errorDump: Boolean = true,
    var errorScreenshot: Boolean = true,
    var errorDumpFilePath: ByDoc[String] = FilePaths.UUIDName(FilePaths.Hierarchical),
    var remoteResourceTimeout: Timeout = Timeout(60.seconds),
    var DFSTimeout: Timeout = Timeout(40.seconds),
    var failOnDFSRead: Boolean = false,
    var defaultFlattenSampler: Sampler[Any] = identity,
    var defaultForkSampler: Sampler[Any] = identity, // join takes remote actions and cost much more than flatten.
    var defaultExploreRange: Range = 0 until Int.MaxValue,
    var defaultGenPartitioner: GenPartitioner = GenPartitioners.Wide(),
    var defaultExploreAlgorithm: PathPlanning = PathPlanners_Simple.BreadthFirst,
    var epochSize: Int = 500,
    var checkpointInterval: Int = 50, // disabled if <=0

    // if encounter too many out of memory error, change to MEMORY_AND_DISK_SER
    var defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
) extends Core.MutableConfLike
    with Serializable {

  override def importFrom(sparkConf: SparkConf): SpookyConf = {
    this
  }

  def getEarliestDocCreationTime(nowMillis: Long = System.currentTimeMillis()): Long = {

    val earliestTimeFromDuration = cachedDocsLifeSpan match {
      case _: Infinite => Long.MinValue
      case d =>
        nowMillis - d.toMillis
    }
    IgnoreCachedDocsBefore match {
      case Some(expire) =>
        Math.max(expire.getTime, earliestTimeFromDuration)
      case None =>
        earliestTimeFromDuration
    }
  }

  def previewMode: this.type = {

    val sampler: Samplers.FirstN = Samplers.FirstN(1)
    this.defaultForkSampler = sampler
    this.defaultForkSampler = sampler
    this.defaultExploreRange = 0 to 2

    this
  }
}
