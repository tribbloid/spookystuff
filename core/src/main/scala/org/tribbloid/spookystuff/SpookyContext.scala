package org.tribbloid.spookystuff

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.dsl.NaiveDriverFactory
import org.tribbloid.spookystuff.entity.{Key, KeyLike, PageRow}
import org.tribbloid.spookystuff.sparkbinding.{PageRowRDD, SchemaRDDView, StringRDDView}
import org.tribbloid.spookystuff.utils.Utils

import scala.collection.immutable.ListSet
import scala.concurrent.duration.{Duration, _}
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 12/06/14.
 */
//class SpookyContext(val sc: SparkContext) {
//
//}

//will be shipped everywhere as implicit parameter

class SpookyContext (
                      //TODO: these should not be imported and littered everywhere
                      @transient val sqlContext: SQLContext, //compulsory, many things are not possible without SQL,

                      val dir: DirConf = new DirConf(),

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

                      var remoteResourceTimeout: Duration = 60.seconds,
                      var DFSTimeout: Duration = 40.seconds,

                      var failOnDFSError: Boolean = false,

                      //default max number of elements scraped from a page, set to Int.MaxValue to allow unlimited fetch
                      var joinLimit: Int = Int.MaxValue,
                      var maxExploreDepth: Int = Int.MaxValue,

                      var defaultQueryOptimizer: QueryOptimizer = Smart,

                      var paginationLimit: Int = 1000 //TODO: deprecate soon
                      )
  extends Serializable {

  import views._

  var metrics = new Metrics()

  def cleanMetrics(): Unit = {
    metrics = new Metrics()
  }

  val hConfWrapper =  if (sqlContext!=null) new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration)
  else null

  def hConf = hConfWrapper.value

  @transient lazy val noInput: PageRowRDD = PageRowRDD(this.sqlContext.sparkContext.noInput, spooky = this)

  def this(sqlContext: SQLContext) {
    this(sqlContext, driverFactory = NaiveDriverFactory())
  }

  def this(sc: SparkContext) {
    this(new SQLContext(sc))
  }

  def this(conf: SparkConf) {
    this(new SQLContext(new SparkContext(conf)))
  }

  implicit def stringRDDToItsView(rdd: RDD[String]): StringRDDView = new StringRDDView(rdd)

  implicit def schemaRDDToItsView(rdd: SchemaRDD): SchemaRDDView = new SchemaRDDView(rdd)

  //  implicit def selfToPageRowRDD(rdd: RDD[PageRow]): PageRowRDD = PageRowRDD(rdd, spooky = this)

  implicit def RDDToPageRowRDD[T: ClassTag](rdd: RDD[T]): PageRowRDD = {
    import views._
    import scala.reflect._
//    import scala.reflect.runtime.universe._

    rdd match {
      case rdd: SchemaRDD =>
        val self = new SchemaRDDView(rdd).asMapRDD.map{
          map =>
            new PageRow(
              Option(map)
                .getOrElse(Map())
                .map(tuple => (Key(tuple._1),tuple._2))
            )
        }
        new PageRowRDD(self, keys = ListSet(rdd.schema.fieldNames: _*).map(Key(_)), spooky = this)
      case _ if classOf[Map[_,_]].isAssignableFrom(classTag[T].runtimeClass) => //use classOf everywhere?
        val canonRdd = rdd.map(
          map =>map.asInstanceOf[Map[_,_]].canonizeKeysToColumnNames
        )

        val jsonRDD = canonRdd.map(
          map =>
            Utils.toJson(map)
        )
        val schemaRDD = sqlContext.jsonRDD(jsonRDD)
        val self = canonRdd.map(
          map =>
            PageRow(map.map(tuple => (Key(tuple._1),tuple._2)), Seq())
        )
        new PageRowRDD(self, keys = ListSet(schemaRDD.schema.fieldNames: _*).map(Key(_)), spooky = this)
      case _ =>
        val self = rdd.map{
          str =>
            var cells = Map[KeyLike,Any]()
            if (str!=null) cells = cells + (Key("_") -> str)

            new PageRow(cells)
        }
        new PageRowRDD(self, keys = ListSet(Key("_")), spooky = this)
    }
  }
}

case class DirConf(
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
  //    def errorScreenshotLocal_=(v: String): Unit = _errorScreenshotLocal = Option(v)

  def autoSave = Option(_autoSave).orElse(rootOption.map(_+"page")).getOrElse("temp/page/")
  def cache = Option(_cache).orElse(rootOption.map(_+"cache")).getOrElse("temp/cache/")
  def errorDump = Option(_errorDump).orElse(rootOption.map(_+"error")).getOrElse("temp/error/")
  def errorScreenshot = Option(_errorScreenshot).orElse(rootOption.map(_+"error-screenshot")).getOrElse("temp/error-screenshot/")
  def checkpoint = Option(_checkpoint).orElse(rootOption.map(_+"checkpoint")).getOrElse("temp/checkpoint/")
  def errorDumpLocal = Option(_errorDumpLocal).getOrElse("temp/error/")
  def errorScreenshotLocal = Option(_errorScreenshotLocal).getOrElse("temp/error-screenshot/")
}

//TODO: metrics should not be broadcasted like rest of SpookyContext!
class Metrics() extends Serializable {

  //works but temporarily disabled as not part of 'official' API
    private def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]) = {
      new Accumulator(initialValue, param, Some(name))
    }

  import org.apache.spark.SparkContext._

  val driverInitialized: Accumulator[Int] = accumulator(0, "driverInitialized")
  val driverReclaimed: Accumulator[Int] = accumulator(0, "driverReclaimed")

  val sessionInitialized: Accumulator[Int] = accumulator(0, "sessionInitialized")
  val sessionReclaimed: Accumulator[Int] = accumulator(0, "sessionReclaimed")

  val DFSReadSuccess: Accumulator[Int] = accumulator(0, "DFSReadSuccess")
  val DFSReadFail: Accumulator[Int] = accumulator(0, "DFSReadFail")

  val DFSWriteSuccess: Accumulator[Int] = accumulator(0, "DFSWriteSuccess")
  val DFSWriteFail: Accumulator[Int] = accumulator(0, "DFSWriteFail")

  val pagesFetched: Accumulator[Int] = accumulator(0, "pagesFetched")
  val pagesFetchedFromWeb: Accumulator[Int] = accumulator(0, "pagesFetchedFromWeb")
  val pagesFetchedFromCache: Accumulator[Int] = accumulator(0, "pagesFetchedFromCache")
}