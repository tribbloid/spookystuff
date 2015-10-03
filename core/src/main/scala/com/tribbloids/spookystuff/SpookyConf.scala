package com.tribbloids.spookystuff

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.expressions.{CacheFilePath, PageFilePath}
import com.tribbloids.spookystuff.session.OAuthKeys
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
}

/**
 * Created by peng on 12/06/14.
 * will be shipped to workers
 */
//TODO: is var in serialized closure unstable for Spark production environment? consider changing to ConcurrentHashMap or merge with SparkConf
class SpookyConf (
                   val dirs: DirConf = new DirConf(),

                   var shareMetrics: Boolean = false, //TODO: not necessary

                   //TODO: 3 of the following functions can be changed to Expressions
                   var driverFactory: DriverFactory = DriverFactories.PhantomJS(),
                   var proxy: ProxyFactory = ProxyFactories.NoProxy,
                   //                   var userAgent: ()=> String = () => null,
                   var userAgent: ()=> String = () => "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                   var headers: ()=> Map[String, String] = () => Map(),
                   var oAuthKeys: () => OAuthKeys = () => null,

                   val browserResolution: (Int, Int) = (1920, 1080),

                   var remote: Boolean = true, //if disabled won't use remote client at all
                   var autoSave: Boolean = true,
                   var cacheWrite: Boolean = true,
                   var cacheRead: Boolean = true,
                   var errorDump: Boolean = true,
                   var errorScreenshot: Boolean = true,

                   var pageExpireAfter: Duration = 7.day,
                   var pageNotExpiredSince: Option[Date] = None,

                   var cachePath: CacheFilePath[String] = CacheFilePaths.Hierarchical,
                   var autoSavePath: PageFilePath[String] = PageFilePaths.UUIDName(CacheFilePaths.Hierarchical),
                   var errorDumpPath: PageFilePath[String] = PageFilePaths.UUIDName(CacheFilePaths.Hierarchical),

                   var defaultParallelism: RDD[_] => Int = Parallelism.PerCore(8),

                   var remoteResourceTimeout: Duration = 60.seconds,
                   var DFSTimeout: Duration = 40.seconds,

                   var failOnDFSError: Boolean = false,

                   val defaultJoinType: JoinType = LeftOuter,

                   //default max number of elements scraped from a page, set to Int.MaxValue to allow unlimited fetch
                   var maxJoinOrdinal: Int = Int.MaxValue,
                   var maxExploreDepth: Int = Int.MaxValue,

                   var defaultQueryOptimizer: QueryOptimizer = Wide,

                   var checkpointInterval: Int = 100,

                   var defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                   ) extends Serializable {

  def importFrom(implicit sparkConf: SparkConf): SpookyConf = {

    val root = Option(this.dirs.root).getOrElse(SpookyConf.getDefault("spooky.dirs.root", "temp"))
    //    def root_/(subdir: String) = Utils.uriSlash(root) + subdir

    val localRoot = Option(this.dirs.root).getOrElse(SpookyConf.getDefault("spooky.dirs.root.local", "temp"))
    //    def localRoot_/(subdir: String) = Utils.uriSlash(root) + subdir

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

      this.driverFactory,
      this.proxy,
      //                   var userAgent: ()=> String = () => null,
      this.userAgent,
      this.headers,
      this.oAuthKeys,

      this.browserResolution,

      this.remote,
      this.autoSave,
      this.cacheWrite,
      this.cacheRead,
      this.errorDump,
      this.errorScreenshot,

      this.pageExpireAfter,
      this.pageNotExpiredSince,

      this.cachePath,
      this.autoSavePath,
      this.errorDumpPath,

      this.defaultParallelism,

      this.remoteResourceTimeout,
      this.DFSTimeout,

      this.failOnDFSError,

      this.defaultJoinType,

      //default max number of elements scraped from a page, set to Int.MaxValue to allow unlimited fetch
      this.maxJoinOrdinal,
      this.maxExploreDepth,

      this.defaultQueryOptimizer,

      this.checkpointInterval,

      this.defaultStorageLevel
    )
  }

//  def toJSON: String = {
//
//    Utils.toJson(this, beautiful = true)
//  }
}