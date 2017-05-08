package com.tribbloids.spookystuff

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.row.Sampler
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.utils.Static
import org.apache.spark.ml.dsl.utils.MessageAPI
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkEnv}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration.Infinite
import scala.concurrent.duration._
import scala.reflect.ClassTag

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

  //  def getDefault(className: String): AnyRef = {
  //
  //    val clazz = Class.forName(className)
  //    val neo = ReflectionUtils.invokeStatic(clazz, "default")
  //    neo
  //  }

  val builderRegistry: ArrayBuffer[Builder[_]] = ArrayBuffer[Builder[_]]() //singleton

  abstract class Builder[T](implicit val ctg: ClassTag[T]) extends Static[T] {

    builderRegistry += this

    def default: T
  }
}

case class Submodules[U] private(
                                  self: mutable.Map[String, U]
                                ) extends Iterable[U] {

  def getOrBuild[T <: U](implicit ev: Submodules.Builder[T]): T = {

    self.get(ev.ctg.runtimeClass.getCanonicalName)
      .map {
        _.asInstanceOf[T]
      }
      .getOrElse {
        val result = ev.default
        self.put(result.getClass.getCanonicalName, result)
        result
      }
  }

  def transform(f: U => U): Submodules[U] = {

    Submodules(self.values.map(f).toSeq: _*)
  }

  override def iterator: Iterator[U] = self.valuesIterator
}

object AbstractConf {

  def getPropertyOrEnv(
                        property: String
                      )(implicit conf: SparkConf = Option(SparkEnv.get).map(_.conf).orNull): Option[String] = {

    val env = property.replace('.','_').toUpperCase

    Option(System.getProperty(property)).filter (_.toLowerCase != "null").map {
      v =>
        LoggerFactory.getLogger(this.getClass).info(s"System has property $property -> $v")
        v
    }
      .orElse {
        Option(System.getenv(env)).filter (_.toLowerCase != "null").map {
          v =>
            LoggerFactory.getLogger(this.getClass).info(s"System has environment $env -> $v")
            v
        }
      }
      .orElse {
        Option(conf) //this is ill-suited for third-party application, still here but has lowest precedence.
          .flatMap(
          _.getOption(property).map {
            v =>
              LoggerFactory.getLogger(this.getClass).info(s"SparkConf has property $property -> $v")
              v
          }
        )
          .filter (_.toLowerCase != "null")
      }
  }

  /**
    * spark config >> system property >> system environment >> default
    */
  def getOrDefault(
                    property: String,
                    default: String = null
                  )(implicit conf: SparkConf = Option(SparkEnv.get).map(_.conf).orNull): String = {

    getPropertyOrEnv(property)
      .getOrElse{
        default
      }
  }
}

/**
  * all subclasses have to define default() in their respective companion object.
  */
trait AbstractConf extends MessageAPI {

  //  val submodules: Submodules[AbstractConf] = Submodules()

  // TODO: use reflection to automate
  protected def importFrom(sparkConf: SparkConf): this.type

  @transient @volatile var sparkConf: SparkConf = _
  def effective: this.type = Option(sparkConf).map {
    conf =>
      val result = importFrom(conf)
      result.sparkConf = this.sparkConf
      result
  }
    .getOrElse(this)
    .asInstanceOf[this.type]

  override def clone: this.type = importFrom(new SparkConf())
}

trait ModuleConf extends AbstractConf {
}

object SpookyConf extends Submodules.Builder[SpookyConf]{

  final val DEFAULT_WEBDRIVER_FACTORY = DriverFactories.PhantomJS().taskLocal

  /**
    * otherwise driver cannot do screenshot
    */
  final val TEST_WEBDRIVER_FACTORY = DriverFactories.PhantomJS(loadImages = true).taskLocal
  final val DEFAULT_PYTHONDRIVER_FACTORY = DriverFactories.Python().taskLocal

  //DO NOT change to val! all confs are mutable
  def default = new SpookyConf()
}

/**
  * Created by peng on 12/06/14.
  * will be shipped to workers
  */
//TODO: is var in serialized closure unstable for Spark production environment? consider changing to ConcurrentHashMap
class SpookyConf(
                //TODO: this should be lifted to the same level in SpookyContext
                  val submodules: Submodules[ModuleConf] = Submodules(),

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

  def submodule[T <: ModuleConf](implicit ev: Submodules.Builder[T]): T = submodules.getOrBuild[T](ev)

  def dirConf: DirConf = submodule[DirConf]

  override def importFrom(sparkConf: SparkConf): this.type = {

    Submodules.builderRegistry.foreach {
      builder =>
        if (classOf[ModuleConf] isAssignableFrom builder.ctg.runtimeClass) {
          this.submodule(builder.asInstanceOf[Submodules.Builder[_ <: ModuleConf]])
        }
    }

    //TODO: eliminate hardcoding! we have 2 options:
    // 1. Use reflection: property name -> package name -> submodule class name
    // 2. lazily loaded by constructor of Actions that use such submodule.
    //    Seq(
    //      classOf[DirConf].getCanonicalName,
    //      "com.tribbloids.spookystuff.mav.UAVConf"
    //    )
    //      .foreach {
    //        name =>
    //          this.submodules.tryGetByName(name)
    //      }

    val transformed = this.submodules.transform {
      v =>
        v.sparkConf = sparkConf
        v.effective
    }

    new SpookyConf(
      transformed,

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