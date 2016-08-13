package com.tribbloids.spookystuff

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.row.Sampler
import com.tribbloids.spookystuff.session.{OAuthKeys, ProxySetting, PythonDriver}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.openqa.selenium.WebDriver

import scala.concurrent.duration.Duration.Infinite
import scala.concurrent.duration._

object SpookyConf {

  private def getDefault(
                          property: String,
                          backup: String = null
                        )(implicit conf: SparkConf): String = {
    val env = property.replace('.','_').toUpperCase

    conf.getOption(property)
      .orElse{
        Option(System.getProperty(property))
      }.orElse{
      Option(System.getenv(env))
    }.getOrElse{
      backup
    }
  }

  final val DEFAULT_WEBDRIVER_FACTORY = DriverFactories.PhantomJS().pooled
  final val DEFAULT_PYTHONDRIVER_FACTORY = DriverFactories.Python().pooled
}

/**
  * Created by peng on 12/06/14.
  * will be shipped to workers
  */
//TODO: is var in serialized closure unstable for Spark production environment? consider changing to ConcurrentHashMap
class SpookyConf (
                   val dirs: DirConf = new DirConf(),

                   var shareMetrics: Boolean = false, //TODO: not necessary

                   var webDriverFactory: DriverFactory[WebDriver] = SpookyConf.DEFAULT_WEBDRIVER_FACTORY,
                   var pythonDriverFactory: DriverFactory[PythonDriver] = SpookyConf.DEFAULT_PYTHONDRIVER_FACTORY,

                   var proxy: () => ProxySetting = ProxyFactories.NoProxy,

                   //TODO: merge into headersFactory
                   var userAgentFactory: () => String = {
                     () => "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36"
                   },
                   var headersFactory: () => Map[String, String] = () => Map(),
                   var oAuthKeysFactory: () => OAuthKeys = () => null,

                   val browserResolution: (Int, Int) = (1920, 1080),

                   var remote: Boolean = true, //if disabled won't use remote client at all
                   var autoSave: Boolean = true,
                   var cacheWrite: Boolean = true,
                   var cacheRead: Boolean = true,
                   var errorDump: Boolean = true,
                   var errorScreenshot: Boolean = true,

                   var docsLifeSpan: Duration = 7.day,
                   var IgnoreDocsCreatedBefore: Option[Date] = None,

                   var cacheFilePath: ByTrace[String] = FilePaths.Hierarchical,
                   var autoSaveFilePath: ByDoc[String] = FilePaths.UUIDName(FilePaths.Hierarchical),
                   var errorDumpFilePath: ByDoc[String] = FilePaths.UUIDName(FilePaths.Hierarchical),

                   var defaultPartitionerFactory: RDD[_] => Partitioner = PartitionerFactories.SameParallelism,

                   var remoteResourceTimeout: Duration = 60.seconds,
                   var DFSTimeout: Duration = 40.seconds,

                   var failOnDFSError: Boolean = false,

                   val defaultJoinType: JoinType = Inner,

                   var defaultFlattenSampler: Sampler[Any] = identity,
                   var defaultJoinSampler: Sampler[Any] = identity, //join takes remote actions and cost much more than flatten.
                   var defaultExploreRange: Range = 0 until Int.MaxValue,

                   var defaultFetchOptimizer: FetchOptimizer = FetchOptimizers.Wide,
                   var defaultExploreAlgorithm: ExploreAlgorithm = ExploreAlgorithms.ShortestPath,

                   var epochSize: Int = 500,
                   var checkpointInterval: Int = -1, //disabled if <=0

                   //if encounter too many out of memory error, change to MEMORY_AND_DISK_SER
                   var defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,

                   var alwaysDownloadBrowserRemotely: Boolean = false //mostly for testing

                 ) extends Serializable {

  def importFrom(sparkContext: SparkContext): SpookyConf = importFrom(sparkContext.getConf)

  def importFrom(implicit sparkConf: SparkConf): SpookyConf = {

    val root = Option(this.dirs.root).getOrElse(SpookyConf.getDefault("spooky.dirs.root", "temp"))
    //    def root_/(subdir: String) = Utils.uriSlash(root) + subdir

    val localRoot = Option(this.dirs.root).getOrElse(SpookyConf.getDefault("spooky.dirs.root.local", "temp"))
    //    def localRoot_/(subdir: String) = Utils.uriSlash(root) + subdir

    //TODO: use Java reflection to generalize system property/variable override rules
    val dirs = new DirConf(
      root,
      localRoot,
      Option(this.dirs._autoSave).getOrElse(SpookyConf.getDefault("spooky.dirs.autosave")),
      Option(this.dirs._cache).getOrElse(SpookyConf.getDefault("spooky.dirs.cache")),
      Option(this.dirs._errorDump).getOrElse(SpookyConf.getDefault("spooky.dirs.error.dump")),
      Option(this.dirs._errorScreenshot).getOrElse(SpookyConf.getDefault("spooky.dirs.error.screenshot")),
      Option(this.dirs._checkpoint).getOrElse(SpookyConf.getDefault("spooky.dirs.checkpoint")),
      Option(this.dirs._errorDumpLocal).getOrElse(SpookyConf.getDefault("spooky.dirs.error.dump.local")),
      Option(this.dirs._errorScreenshotLocal).getOrElse(SpookyConf.getDefault("spooky.dirs.error.screenshot.local"))
    )

    new SpookyConf(
      dirs,

      this.shareMetrics,

      this.webDriverFactory,
      this.pythonDriverFactory,
      this.proxy,
      //                   var userAgent: ()=> String = () => null,
      this.userAgentFactory,
      this.headersFactory,
      this.oAuthKeysFactory,

      this.browserResolution,

      this.remote,
      this.autoSave,
      this.cacheWrite,
      this.cacheRead,
      this.errorDump,
      this.errorScreenshot,

      this.docsLifeSpan,
      this.IgnoreDocsCreatedBefore,

      this.cacheFilePath,
      this.autoSaveFilePath,
      this.errorDumpFilePath,

      this.defaultPartitionerFactory,

      this.remoteResourceTimeout,
      this.DFSTimeout,

      this.failOnDFSError,

      this.defaultJoinType,

      //default max number of elements scraped from a page, set to Int.MaxValue to allow unlimited fetch
      this.defaultFlattenSampler,
      this.defaultJoinSampler,

      this.defaultExploreRange,

      this.defaultFetchOptimizer,
      this.defaultExploreAlgorithm,

      this.epochSize,
      this.checkpointInterval,

      this.defaultStorageLevel,

      this.alwaysDownloadBrowserRemotely
    )
  }

  def getEarliestDocCreationTime(nowMillis: Long = System.currentTimeMillis()): Long = {

    val earliestTimeFromDuration = docsLifeSpan match {
      case inf: Infinite => Long.MinValue
      case d =>
        nowMillis - d.toMillis
    }
    IgnoreDocsCreatedBefore match {
      case Some(expire) =>
        Math.max(expire.getTime, earliestTimeFromDuration)
      case None =>
        earliestTimeFromDuration
    }
  }

  //  def toJSON: String = {
  //
  //    Utils.toJson(this, beautiful = true)
  //  }
}