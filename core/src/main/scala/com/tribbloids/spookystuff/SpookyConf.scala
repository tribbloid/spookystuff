package com.tribbloids.spookystuff

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.expressions.{CacheFilePath, PageFilePath}
import com.tribbloids.spookystuff.session.OAuthKeys
import com.tribbloids.spookystuff.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.concurrent.duration._

/**
 * Created by peng on 2/2/15.
 */
class DirConf(
               var root: String = null,//ystem.getProperty("spooky.dirs.root"),
               var localRoot: String = null,
               var _autoSave: String = null,//System.getProperty("spooky.dirs.autosave"),
               var _cache: String = null,//System.getProperty("spooky.dirs.cache"),
               var _errorDump: String = null,//System.getProperty("spooky.dirs.errordump"),
               var _errorScreenshot: String = null,//System.getProperty("spooky.dirs.errorscreenshot"),
               var _checkpoint: String = null,//System.getProperty("spooky.dirs.checkpoint"),
               var _errorDumpLocal: String = null,//System.getProperty("spooky.dirs.errordump.local"),
               var _errorScreenshotLocal: String = null//System.getProperty("spooky.dirs.errorscreenshot.local")
               ) extends Serializable {

  def root_/(subdir: String): String = Utils.uriSlash(root) + subdir
  def localRoot_/(subdir: String) = Utils.uriSlash(root) + subdir

  def autoSave_=(v: String): Unit = _autoSave = v
  def cache_=(v: String): Unit = _cache = v
  def errorDump_=(v: String): Unit = _errorDump = v
  def errorScreenshot_=(v: String): Unit = _errorScreenshot = v
  def checkpoint_=(v: String): Unit = _checkpoint = v
  def errorDumpLocal_=(v: String): Unit = _errorDumpLocal = v
  def errorScreenshotLocal_=(v: String): Unit = _errorScreenshotLocal = v

  def autoSave: String = Option(_autoSave).getOrElse(root_/("autosave"))
  def cache: String = Option(_autoSave).getOrElse(root_/("cache"))
  def errorDump: String = Option(_autoSave).getOrElse(root_/("errorDump"))
  def errorScreenshot: String = Option(_autoSave).getOrElse(root_/("errorScreenshot"))
  def checkpoint: String = Option(_autoSave).getOrElse(root_/("checkpoint"))
  def errorDumpLocal: String = Option(_autoSave).getOrElse(localRoot_/("errorDump"))
  def errorScreenshotLocal: String = Option(_autoSave).getOrElse(localRoot_/("errorScreenshot"))

  def toJSON: String = {

    Utils.toJson(this, beautiful = true)
  }
}

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

    new SpookyConf(
      this.dirs,

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