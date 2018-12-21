package com.tribbloids.spookystuff.conf

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.row.Sampler
import com.tribbloids.spookystuff.session.{PythonDriver, _}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import scala.concurrent.duration.Duration.Infinite
import scala.concurrent.duration._

object SpookyConf extends Submodules.Builder[SpookyConf] {

  final val DEFAULT_WEBDRIVER_FACTORY = DriverFactories.PhantomJS().taskLocal

  /**
    * otherwise driver cannot do screenshot
    */
  final val TEST_WEBDRIVER_FACTORY = DriverFactories.PhantomJS(loadImages = true).taskLocal
  final val DEFAULT_PYTHONDRIVER_FACTORY = DriverFactories.Python2.taskLocal

  //DO NOT change to val! all confs are
  // mutable
  def default = new SpookyConf()

  val defaultHTTPHeaders = Map[String, String](
    "User-Agent" ->
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36"
  )
}

/**
  * Created by peng on 12/06/14.
  * will be shipped to workers
  */
//TODO: should be case class
class SpookyConf(
    var shareMetrics: Boolean = false, //TODO: not necessary

    var webDriverFactory: DriverFactory[CleanWebDriver] = SpookyConf.DEFAULT_WEBDRIVER_FACTORY,
    var webProxy: WebProxyFactory = WebProxyFactories.NoProxy,
    var httpHeadersFactory: () => Map[String, String] = () => SpookyConf.defaultHTTPHeaders,
    var oAuthKeysFactory: () => OAuthKeys = () => null,
    var browserResolution: (Int, Int) = (1920, 1080),
    var pythonDriverFactory: DriverFactory[PythonDriver] = SpookyConf.DEFAULT_PYTHONDRIVER_FACTORY,
    var remote: Boolean = true, //if disabled won't use remote client at all
    var autoSave: Boolean = true,
    var cacheWrite: Boolean = true,
    var cacheRead: Boolean = true, //TODO: this enable both in-memory and DFS cache, should allow more refined control

    var cachedDocsLifeSpan: Duration = 7.day,
    var IgnoreCachedDocsBefore: Option[Date] = None,
    var cacheFilePath: ByTrace[String] = FilePaths.Hierarchical,
    var autoSaveFilePath: ByDoc[String] = FilePaths.UUIDName(FilePaths.Hierarchical),
    var errorDump: Boolean = true,
    var errorScreenshot: Boolean = true,
    var errorDumpFilePath: ByDoc[String] = FilePaths.UUIDName(FilePaths.Hierarchical),
    var remoteResourceTimeout: Duration = 60.seconds,
    var DFSTimeout: Duration = 40.seconds,
    var failOnDFSError: Boolean = false,
    var defaultJoinType: JoinType = Inner,
    var defaultFlattenSampler: Sampler[Any] = identity,
    var defaultJoinSampler: Sampler[Any] = identity, //join takes remote actions and cost much more than flatten.
    var defaultExploreRange: Range = 0 until Int.MaxValue,
    var defaultGenPartitioner: GenPartitioner = GenPartitioners.Wide(),
    var defaultExploreAlgorithm: ExploreAlgorithm = ExploreAlgorithms.BreadthFirst,
    var epochSize: Int = 500,
    var checkpointInterval: Int = 50, //disabled if <=0

    //if encounter too many out of memory error, change to MEMORY_AND_DISK_SER
    var defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
) extends AbstractConf
    with Serializable {

  override def importFrom(sparkConf: SparkConf): this.type = {

    new SpookyConf(
      shareMetrics = this.shareMetrics,
      webDriverFactory = this.webDriverFactory,
      webProxy = this.webProxy,
      httpHeadersFactory = this.httpHeadersFactory,
      oAuthKeysFactory = this.oAuthKeysFactory,
      browserResolution = this.browserResolution,
      pythonDriverFactory = this.pythonDriverFactory,
      remote = this.remote,
      autoSave = this.autoSave,
      cacheWrite = this.cacheWrite,
      cacheRead = this.cacheRead,
      errorDump = this.errorDump,
      errorScreenshot = this.errorScreenshot,
      cachedDocsLifeSpan = this.cachedDocsLifeSpan,
      IgnoreCachedDocsBefore = this.IgnoreCachedDocsBefore,
      cacheFilePath = this.cacheFilePath,
      autoSaveFilePath = this.autoSaveFilePath,
      errorDumpFilePath = this.errorDumpFilePath,
      remoteResourceTimeout = this.remoteResourceTimeout,
      DFSTimeout = this.DFSTimeout,
      failOnDFSError = this.failOnDFSError,
      defaultJoinType = this.defaultJoinType,
      //default max number of elements scraped from a page, set to Int.MaxValue to allow unlimited fetch
      defaultFlattenSampler = this.defaultFlattenSampler,
      defaultJoinSampler = this.defaultJoinSampler,
      defaultExploreRange = this.defaultExploreRange,
      defaultGenPartitioner = this.defaultGenPartitioner,
      defaultExploreAlgorithm = this.defaultExploreAlgorithm,
      epochSize = this.epochSize,
      checkpointInterval = this.checkpointInterval,
      defaultStorageLevel = this.defaultStorageLevel
    ).asInstanceOf[this.type]
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

  def driverFactories: Seq[DriverFactory[_]] = {
    Seq(
      webDriverFactory,
      pythonDriverFactory
    ).flatMap(v => Option(v))
  }
}

object DefaultSpookyConf extends SpookyConf()
