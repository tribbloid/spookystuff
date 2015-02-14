package org.tribbloid.spookystuff

import org.apache.spark.storage.StorageLevel
import org.tribbloid.spookystuff.SpookyConf.Dirs
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.sparkbinding.PageRowRDD
import org.tribbloid.spookystuff.utils.Utils

import scala.concurrent.duration._

/**
 * Created by peng on 2/2/15.
 */
object SpookyConf {

  class Dirs(
              var root: String = System.getProperty("spooky.root"),
              var _autoSave: String = System.getProperty("spooky.autosave"),
              var _cache: String = System.getProperty("spooky.cache"),
              var _errorDump: String = System.getProperty("spooky.error.dump"),
              var _errorScreenshot: String = System.getProperty("spooky.error.screenshot"),
              var _checkpoint: String = System.getProperty("spooky.checkpoint"),
              var _errorDumpLocal: String = System.getProperty("spooky.error.dump.local"),
              var _errorScreenshotLocal: String = System.getProperty("spooky.error.screenshot.local")
              ) extends Serializable {

    def setRoot(v: String): Unit = {root = v}

    def rootOption = Option(root)

    //    def root_=(v: String): Unit = _root = Option(v)
    //    def autoSave_=(v: String): Unit = _autoSave = Option(v)
    //    def cache_=(v: String): Unit = _cache = Option(v)
    //    def errorDump_=(v: String): Unit = _errorDump = Option(v)
    //    def errorScreenshot_=(v: String): Unit = _errorScreenshot = Option(v)
    //    def checkpoint_=(v: String): Unit = _checkpoint = Option(v)
    //    def errorDumpLocal_=(v: String): Unit = _errorDumpLocal = Option(v)
    //    def errorScreenshotLocal_=(v: String): Unit = _errorScreenshotLocal = Option(v)Utils.urlSlash(

    def autoSave: String = Utils.uriSlash(Option(_autoSave).orElse(rootOption.map(_+"page/")).getOrElse("temp/page/"))
    def cache: String = Utils.uriSlash(Option(_cache).orElse(rootOption.map(_+"cache/")).getOrElse("temp/cache/"))
    def errorDump: String = Utils.uriSlash(Option(_errorDump).orElse(rootOption.map(_+"error/")).getOrElse("temp/error/"))
    def errorScreenshot: String = Utils.uriSlash(Option(_errorScreenshot).orElse(rootOption.map(_+"error-screenshot/")).getOrElse("temp/error-screenshot/"))
    def checkpoint: String = Utils.uriSlash(Option(_checkpoint).orElse(rootOption.map(_+"checkpoint/")).getOrElse("temp/checkpoint/"))
    def errorDumpLocal: String = Utils.uriSlash(Option(_errorDumpLocal).getOrElse("temp/error/"))
    def errorScreenshotLocal: String = Utils.uriSlash(Option(_errorScreenshotLocal).getOrElse("temp/error-screenshot/"))
  }
}

/**
 * Created by peng on 12/06/14.
 */
//class SpookyContext(val sc: SparkContext) {
//
//}

//will be shipped everywhere as implicit parameter

class SpookyConf (
                   val dirs: Dirs = new Dirs(),

                   var sharedMetrics: Boolean = false,

                   var driverFactory: DriverFactory = NaiveDriverFactory(),
                   var proxy: ProxyFactory = NoProxyFactory,
                   var userAgent: ()=> String = () => null,
                   //  val userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                   var headers: ()=> Map[String, String] = () => Map(),
                   val browserResolution: (Int, Int) = (1920, 1080),

                   var autoSave: Boolean = true,
                   var cacheWrite: Boolean = true,
                   var cacheRead: Boolean = true,
                   var errorDump: Boolean = true,
                   var errorScreenshot: Boolean = true,

                   var pageExpireAfter: Duration = 7.day,

                   var autoSaveExtract: Extract[String] = new UUIDFileName(Hierarchical),
                   var cacheTraceEncoder: TraceEncoder[String] = Hierarchical,
                   var errorDumpExtract: Extract[String] = new UUIDFileName(Hierarchical),

                   var defaultParallelism: PageRowRDD => Int = {
                     row =>
                       row.sparkContext.defaultParallelism * 2
                   },

                   var remoteResourceTimeout: Duration = 60.seconds,
                   var DFSTimeout: Duration = 40.seconds,

                   var failOnDFSError: Boolean = false,

                   //default max number of elements scraped from a page, set to Int.MaxValue to allow unlimited fetch
                   var maxJoinOrdinal: Int = Int.MaxValue,
                   var maxExploreDepth: Int = Int.MaxValue,

                   var defaultQueryOptimizer: QueryOptimizer = Wide,

                   var checkpointInterval: Int = 50,
                   var batchSize: Int = 500,

                   var defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
                   ) extends Serializable {

  //  def toJSON: String = { //useless for non-case class
  //
  //    Utils.toJson(this, beautiful = true)
  //  }
}