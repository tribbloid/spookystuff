package org.tribbloid.spookystuff

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.tribbloid.spookystuff.SpookyConf.Dirs
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.expressions.{CacheFilePath, PageFilePath}
import org.tribbloid.spookystuff.session.OAuthKeys
import org.tribbloid.spookystuff.utils.Utils

import scala.concurrent.duration._

/**
 * Created by peng on 2/2/15.
 */
object SpookyConf {

  private def getDefault(property: String, backup: String)(implicit conf: SparkConf): String = {
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

  class Dirs(
              var root: String = null,//ystem.getProperty("spooky.dirs.root"),
              var autoSave: String = null,//System.getProperty("spooky.dirs.autosave"),
              var cache: String = null,//System.getProperty("spooky.dirs.cache"),
              var errorDump: String = null,//System.getProperty("spooky.dirs.errordump"),
              var errorScreenshot: String = null,//System.getProperty("spooky.dirs.errorscreenshot"),
              var checkpoint: String = null,//System.getProperty("spooky.dirs.checkpoint"),
              var errorDumpLocal: String = null,//System.getProperty("spooky.dirs.errordump.local"),
              var errorScreenshotLocal: String = null//System.getProperty("spooky.dirs.errorscreenshot.local")
              ) extends Serializable {

    //    def setRoot(v: String): Unit = {root = v}
    //
    //    def rootOption = Option(root)
    //
    //    def autoSave_=(v: String): Unit = _autoSave = v
    //    def cache_=(v: String): Unit = _cache = v
    //    def errorDump_=(v: String): Unit = _errorDump = v
    //    def errorScreenshot_=(v: String): Unit = _errorScreenshot = v
    //    def checkpoint_=(v: String): Unit = _checkpoint = v
    //    def errorDumpLocal_=(v: String): Unit = _errorDumpLocal = v
    //    def errorScreenshotLocal_=(v: String): Unit = _errorScreenshotLocal = v
    //
    //    def autoSave: String = Utils.uriSlash(Option(_autoSave).orElse(rootOption.map(_+"page/")).getOrElse("temp/page/"))
    //    def cache: String = Utils.uriSlash(Option(_cache).orElse(rootOption.map(_+"cache/")).getOrElse("temp/cache/"))
    //    def errorDump: String = Utils.uriSlash(Option(_errorDump).orElse(rootOption.map(_+"error/")).getOrElse("temp/error/"))
    //    def errorScreenshot: String = Utils.uriSlash(Option(_errorScreenshot).orElse(rootOption.map(_+"error-screenshot/")).getOrElse("temp/error-screenshot/"))
    //    def checkpoint: String = Utils.uriSlash(Option(_checkpoint).orElse(rootOption.map(_+"checkpoint/")).getOrElse("temp/checkpoint/"))
    //    def errorDumpLocal: String = Utils.uriSlash(Option(_errorDumpLocal).getOrElse("temp/error/"))
    //    def errorScreenshotLocal: String = Utils.uriSlash(Option(_errorScreenshotLocal).getOrElse("temp/error-screenshot/"))
  }
}

/**
 * Created by peng on 12/06/14.
 * will be shipped to workers
 */
//TODO: is var in serialized closure unstable for Spark production environment? consider changing to ConcurrentHashMap or merge with SparkConf
class SpookyConf (
                   val dirs: Dirs = new Dirs(),

                   var shareMetrics: Boolean = false, //TODO: not necessary

                   //TODO: 3 of the following functions can be changed to Expressions
                   var driverFactory: DriverFactory = DriverFactories.PhantomJS(),
                   var proxy: ProxyFactory = ProxyFactories.NoProxy,
                   //                   var userAgent: ()=> String = () => null,
                   var userAgent: ()=> String = () => "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                   var headers: ()=> Map[String, String] = () => Map(),
                   var oAuthKeys: () => OAuthKeys = () => null,

                   val browserResolution: (Int, Int) = (1920, 1080),

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

  def inject(sc: SparkContext): SpookyConf = {
    implicit val sparkConf = sc.getConf

    val root = Option(this.dirs.root).getOrElse(SpookyConf.getDefault("spooky.dirs.root", "temp"))

    def root_/(subdir: String) = Utils.uriSlash(root) + subdir

    val dirs = new Dirs(
      root,
      Option(this.dirs.autoSave).getOrElse(SpookyConf.getDefault("spooky.dirs.autosave", root_/("autosave"))),
      Option(this.dirs.cache).getOrElse(SpookyConf.getDefault("spooky.dirs.cache", root_/("cache"))),
      Option(this.dirs.errorDump).getOrElse(SpookyConf.getDefault("spooky.dirs.error.dump", root_/("errorDump"))),
      Option(this.dirs.errorScreenshot).getOrElse(SpookyConf.getDefault("spooky.dirs.error.screenshot", root_/("errorScreenshot"))),
      Option(this.dirs.checkpoint).getOrElse(SpookyConf.getDefault("spooky.dirs.checkpoint", root_/("checkpoint"))),
      Option(this.dirs.errorDumpLocal).getOrElse(SpookyConf.getDefault("spooky.dirs.error.dump.local", root_/("errorDump"))),
      Option(this.dirs.errorScreenshotLocal).getOrElse(SpookyConf.getDefault("spooky.dirs.error.screenshot.local", root_/("errorScreenshot")))
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


}