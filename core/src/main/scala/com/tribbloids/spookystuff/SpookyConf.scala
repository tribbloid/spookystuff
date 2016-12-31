package com.tribbloids.spookystuff

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.row.Sampler
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.session.python.PythonDriver
import org.apache.spark.SparkConf
import org.apache.spark.ml.dsl.ReflectionUtils
import org.apache.spark.ml.dsl.utils.Message
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.concurrent.duration.Duration.Infinite
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Try

object Submodules {

  def apply[U](
                vs: U*
              ): Submodules[U] = {

    Submodules[U](
      mutable.Map(
        vs.map {
          v =>
            v.getClass.getCanonicalName -> v
        }: _*
      )
    )
  }
}

case class Submodules[U] private(
                                  self: mutable.Map[String, U]
                                ) extends Iterable[U] {

  def tryGetByName(
                    className: String,
                    fn: Any => Any = identity
                  ): Try[U] = Try {
    self
      .getOrElse (
        className,
        {
          val clazz = Class.forName(className)
          val neo = ReflectionUtils.invokeStatic(clazz, "default") //TODO: change to apply?
        val result = fn(neo).asInstanceOf[U]
          self.put(className, result)
          result
        }
      )
  }

  def get[T <: U: ClassTag](
                             fn: Any => Any = identity
                           ): T = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val className = clazz.getCanonicalName
    tryGetByName(className, fn)
      .map(_.asInstanceOf[T])
      .get
  }

  def transform(f: U => U) = {

    Submodules(self.values.map(f).toSeq: _*)
  }

  override def iterator: Iterator[U] = self.valuesIterator
}

/**
  * all subclasses have to define default() in their respective companion object.
  */
trait AbstractConf extends Message {

  val submodules: Submodules[AbstractConf] = Submodules()

  // TODO: use reflection to automate
  def importFrom(implicit sparkConf: SparkConf): this.type

  //  def getAndImport[T <: AbstractConf: ClassTag](sparkConfOpt: Option[SparkConf]) = {
  //    sparkConfOpt match {
  //      case None =>
  //        this.components.get[T]()
  //      case Some(conf) =>
  //        this.components.get[T](_.importFrom(conf).asInstanceOf[T])
  //    }
  //  }
}

object SpookyConf {

  /**
    * spark config >> system property >> system environment >> default
    */
  def getPropertyOrDefault(
                            property: String,
                            default: String = null
                          )(implicit conf: SparkConf = null): String = {

    val env = property.replace('.','_').toUpperCase

    Option(conf)
      .flatMap(
        _.getOption(property)
      )
      .orElse{
        Option(System.getProperty(property))
      }
      .orElse{
        Option(System.getenv(env))
      }
      .getOrElse{
        default
      }
  }

  final val DEFAULT_WEBDRIVER_FACTORY = DriverFactories.PhantomJS().pooling

  /**
    * otherwise driver cannot do screenshot
    */
  final val TEST_WEBDRIVER_FACTORY = DriverFactories.PhantomJS(loadImages = true).pooling
  final val DEFAULT_PYTHONDRIVER_FACTORY = DriverFactories.Python().pooling
}

/**
  * Created by peng on 12/06/14.
  * will be shipped to workers
  */
//TODO: is var in serialized closure unstable for Spark production environment? consider changing to ConcurrentHashMap
class SpookyConf (
                   override val submodules: Submodules[AbstractConf] = Submodules(),

                   var shareMetrics: Boolean = false, //TODO: not necessary

                   var webDriverFactory: DriverFactory[CleanWebDriver] = SpookyConf.DEFAULT_WEBDRIVER_FACTORY,
                   var pythonDriverFactory: DriverFactory[PythonDriver] = SpookyConf.DEFAULT_PYTHONDRIVER_FACTORY,

                   var proxy: () => WebProxySetting = WebProxyFactories.NoProxy,

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
                   var cacheRead: Boolean = true, //TODO: this enable both in-memory and DFS cache, should allow more refined control
                   var errorDump: Boolean = true,
                   var errorScreenshot: Boolean = true,

                   var cachedDocsLifeSpan: Duration = 7.day,
                   var IgnoreCachedDocsBefore: Option[Date] = None,

                   var cacheFilePath: ByTrace[String] = FilePaths.Hierarchical,
                   var autoSaveFilePath: ByDoc[String] = FilePaths.UUIDName(FilePaths.Hierarchical),
                   var errorDumpFilePath: ByDoc[String] = FilePaths.UUIDName(FilePaths.Hierarchical),

                   var remoteResourceTimeout: Duration = 60.seconds,
                   var DFSTimeout: Duration = 40.seconds,

                   var failOnDFSError: Boolean = false,

                   val defaultJoinType: JoinType = Inner,

                   var defaultFlattenSampler: Sampler[Any] = identity,
                   var defaultJoinSampler: Sampler[Any] = identity, //join takes remote actions and cost much more than flatten.
                   var defaultExploreRange: Range = 0 until Int.MaxValue,

                   var defaultGenPartitioner: GenPartitioner = GenPartitioners.Wide(),
                   var defaultExploreAlgorithm: ExploreAlgorithm = ExploreAlgorithms.ShortestPath,

                   var epochSize: Int = 500,
                   var checkpointInterval: Int = -1, //disabled if <=0

                   //if encounter too many out of memory error, change to MEMORY_AND_DISK_SER
                   var defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                 ) extends AbstractConf with Serializable {

  def dirConf: DirConf = submodules.get[DirConf]()

  def importFrom(implicit sparkConf: SparkConf): this.type = {

    //TODO: eliminate hardcoding! we have 2 options:
    // 1. Use reflection: property name -> package name -> submodule class name
    // 2. lazily loaded by constructor of Actions that use such submodule.
    Seq(
      classOf[DirConf].getCanonicalName,
      "com.tribbloids.spookystuff.mav.MavConf"
    )
      .foreach {
        name =>
          this.submodules.tryGetByName(name)
      }

    new SpookyConf(
      this.submodules.transform(_.importFrom(sparkConf)),

      shareMetrics = this.shareMetrics,

      webDriverFactory = this.webDriverFactory,
      pythonDriverFactory = this.pythonDriverFactory,
      proxy = this.proxy,
      //                   var userAgent: ()=> String = () => null,
      userAgentFactory = this.userAgentFactory,
      headersFactory = this.headersFactory,
      oAuthKeysFactory = this.oAuthKeysFactory,

      browserResolution = this.browserResolution,

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
    )
      .asInstanceOf[this.type]
  }

  def getEarliestDocCreationTime(nowMillis: Long = System.currentTimeMillis()): Long = {

    val earliestTimeFromDuration = cachedDocsLifeSpan match {
      case inf: Infinite => Long.MinValue
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
    )
      .flatMap(v => Option(v))
  }
}