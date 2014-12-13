package org.tribbloid.spookystuff

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark.{Accumulator, SerializableWritable, SparkConf, SparkContext}
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.dsl.NaiveDriverFactory
import org.tribbloid.spookystuff.entity.{Key, KeyLike, PageRow}
import org.tribbloid.spookystuff.sparkbinding.{PageRowRDD, SchemaRDDView, StringRDDView}
import org.tribbloid.spookystuff.utils.Utils

import scala.collection.immutable.ListSet
import scala.concurrent.duration.{Duration, _}
import scala.language.implicitConversions

/**
 * Created by peng on 12/06/14.
 */
//class SpookyContext(val sc: SparkContext) {
//
//}

//will be shipped everywhere as implicit parameter

class SpookyContext (
                      //TODO: these should not be imported and littered everywhere
                      @transient val sqlContext: SQLContext, //compulsory, many things are not possible without SQL

                      var driverFactory: DriverFactory = NaiveDriverFactory(),
                      var proxy: ()=> ProxySetting =  () => null,
                      var userAgent: ()=> String = () => null,
                      //  val userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                      var headers: ()=> Map[String, String] = () => Map(),
                      val browserResolution: (Int, Int) = (1920, 1080),

                      var autoSave: Boolean = true,//slow, for debugging only
                      var autoCache: Boolean = true,//slow, but reduce bombarding highly aware sites
                      var autoRestore: Boolean = true,//slow, but reduce bombarding highly aware sites
                      var errorDump: Boolean = true,
                      var errorDumpScreenshot: Boolean = true,

                      var pageExpireAfter: Duration = 7.day,

                      var autoSaveExtract: Extract[String] = new UUIDFileName(Hierarchical),
                      var cacheTraceEncoder: TraceEncoder[String] = Hierarchical,
                      var errorDumpExtract: Extract[String] = new UUIDFileName(Hierarchical),

                      var autoSaveRoot: String = "s3n://spooky-page/",
                      var autoCacheRoot: String = "s3n://spooky-cache/",
                      var errorDumpRoot: String = "s3n://spooky-error/",
                      var errorDumpScreenshotRoot: String = "s3n://spooky-error-screenshot/",
                      var localErrorDumpRoot: String = "file:///spooky-error/",
                      var localErrorDumpScreenshotRoot: String = "file:///spooky-error-screenshot/",

                      var remoteResourceTimeout: Duration = 60.seconds,
                      var DFSTimeout: Duration = 60.seconds,

                      var failOnDFSError: Boolean = false,

                      //default max number of elements scraped from a page, set to Int.MaxValue to allow unlimited fetch
                      var joinLimit: Int = Int.MaxValue,
                      var maxExploreDepth: Int = Int.MaxValue,
                      var paginationLimit: Int = 1000 //TODO: deprecate soon
                      )
  extends Serializable {

  var metrics = new Metrics

  def cleanMetrics(): Unit = {
    metrics = new Metrics
  }
  
  val hConfWrapper =  if (sqlContext!=null) new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration)
  else null

  def hConf = hConfWrapper.value

  @transient lazy val noInput: PageRowRDD = new PageRowRDD(this.sqlContext.sparkContext.parallelize(Seq(PageRow())),spooky = this)

  //  implicit def self: SpookyContext = this

  def setRoot(root: String): SpookyContext = {

    autoSaveRoot = root+"page"
    autoCacheRoot = root+"cache"
    errorDumpRoot = root+"error"
    errorDumpScreenshotRoot = root+"error-screenshot"

    this
  }

  def this(conf: SparkConf) {
    this(new SQLContext(new SparkContext(conf)))
  }

  implicit def stringRDDToItsFunctions(rdd: RDD[String]): StringRDDView = new StringRDDView(rdd)

  implicit def schemaRDDToItsFunctions(rdd: SchemaRDD): SchemaRDDView = new SchemaRDDView(rdd)

  //  implicit def pageRowRDDToItsFunctions[T <% RDD[PageRow]](rdd: T) = new PageRowRDD(rdd)(this)

  //these are the entry points of SpookyStuff starting from a common RDD of strings or maps
  implicit def stringRDDToPageRowRDD(rdd: RDD[String]): PageRowRDD = {
    val result = rdd.map{
      str => {
        var cells = Map[KeyLike,Any]()
        if (str!=null) cells = cells + (Key("_") -> str)

        new PageRow(cells)
      }
    }

    new PageRowRDD(result, keys = ListSet(Key("_")), spooky = this)
  }

  implicit def schemaRDDToPageRowRDD(rdd: SchemaRDD): PageRowRDD = {

    val result = new SchemaRDDView(rdd).asMapRDD.map{
      map => new PageRow(Option(map).getOrElse(Map()).map(tuple => (Key(tuple._1),tuple._2)))
    }

    new PageRowRDD(result, keys = ListSet(rdd.schema.fieldNames: _*).map(Key(_)), spooky = this)
  }

  //TODO: doesn't preserve order, impossible without applySchema api
  implicit def mapRDDToPageRowRDD[T <: Any](rdd: RDD[Map[String,T]]): PageRowRDD = {

    import views._

    val jsonRDD = rdd.map(map => Utils.toJson(map.canonizeKeysToColumnNames))
    jsonRDD.persist()
    val schemaRDD = sqlContext.jsonRDD(jsonRDD)

    schemaRDDToPageRowRDD(schemaRDD)
  }

  class Metrics extends Serializable {

    private def sc = SpookyContext.this.sqlContext.sparkContext

    import org.apache.spark.SparkContext._

    val driverInitialized: Accumulator[Int] = sc.accumulator(0, "driverInitialized")
    val driverReclaimed: Accumulator[Int] = sc.accumulator(0, "driverReclaimed")

    val sessionInitialized: Accumulator[Int] = sc.accumulator(0, "sessionInitialized")
    val sessionReclaimed: Accumulator[Int] = sc.accumulator(0, "sessionReclaimed")

    val DFSReadSuccess: Accumulator[Int] = sc.accumulator(0, "DFSReadSuccess")
    val DFSReadFail: Accumulator[Int] = sc.accumulator(0, "DFSReadFail")

    val DFSWriteSuccess: Accumulator[Int] = sc.accumulator(0, "DFSWriteSuccess")
    val DFSWriteFail: Accumulator[Int] = sc.accumulator(0, "DFSWriteFail")

    val pagesFetchedFromCache: Accumulator[Int] = sc.accumulator(0, "pagesFetchedFromCache")
    val pagesFetched: Accumulator[Int] = sc.accumulator(0, "pagesFetched")
  }
}