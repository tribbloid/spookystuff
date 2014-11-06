package org.tribbloid.spookystuff

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SerializableWritable, SparkConf, SparkContext}
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.factory.driver.{DriverFactory, NaiveDriverFactory}
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.sparkbinding.{SchemaRDDFunctions, PageSchemaRDD, StringRDDFunctions}
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
                      //TODO: all declare private to avoid being imported and littered everywhere
                      @transient val sqlContext: SQLContext, //compulsory, many things are not possible without SQL

                      var driverFactory: DriverFactory = NaiveDriverFactory(),

                      var autoSave: Boolean = true,//slow, for debugging only
                      var autoCache: Boolean = true,//slow, but reduce bombarding highly aware sites
                      var autoRestore: Boolean = true,//slow, but reduce bombarding highly aware sites
                      var errorDump: Boolean = true,
                      var errorDumpScreenshot: Boolean = true,

                      var pageExpireAfter: Duration = 7.day,

                      var autoSaveExtract: Extract[_] = UUIDPath(HierarchicalUrnEncoder),
                      var cacheTraceEncoder: TraceEncoder[_] = HierarchicalUrnEncoder,
                      var errorDumpExtract: Extract[_] = UUIDPath(HierarchicalUrnEncoder),

                      var autoSaveRoot: String = "s3n://spooky-page/",
                      var autoCacheRoot: String = "s3n://spooky-cache/",
                      var errorDumpRoot: String = "s3n://spooky-error/",
                      var errorDumpScreenshotRoot: String = "s3n://spooky-error-screenshot/",
                      var localErrorDumpRoot: String = "file:///spooky-error/",
                      var localErrorDumpScreenshotRoot: String = "file:///spooky-error-screenshot/",

                      var remoteResourceTimeout: Duration = 120.seconds,
                      var DFSTimeout: Duration = 60.seconds,

                      var failOnDFSError: Boolean = false,

                      //default max number of elements scraped from a page, set to Int.MaxValue to allow unlimited fetch
                      var joinLimit: Int = Int.MaxValue,
                      var sliceLimit: Int = Int.MaxValue,
                      var paginationLimit: Int = Int.MaxValue,

                      var recursionDepth: Int = 500, //unknown if it is enough
                      val browserResolution: (Int, Int) = (1920, 1080)
                      )
  extends Serializable {

  val hConfWrapper =  if (sqlContext!=null) new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration)
  else null

  def hConf = hConfWrapper.value

  @transient lazy val noInput: PageSchemaRDD = new PageSchemaRDD(this.sqlContext.sparkContext.parallelize(Seq(PageRow())),spooky = this)

  //  implicit def self: SpookyContext = this

  def setRoot(root: String): Unit = {

    autoSaveRoot = root+"page"
    autoCacheRoot = root+"cache"
    errorDumpRoot = root+"error"
    errorDumpScreenshotRoot = root+"error-screenshot"
  }

  def this(conf: SparkConf) {
    this(new SQLContext(new SparkContext(conf)))
  }

  implicit def stringRDDToItsFunctions(rdd: RDD[String]): StringRDDFunctions = new StringRDDFunctions(rdd)

  implicit def schemaRDDToItsFunctions(rdd: SchemaRDD): SchemaRDDFunctions = new SchemaRDDFunctions(rdd)

  //  implicit def pageRowRDDToItsFunctions[T <% RDD[PageRow]](rdd: T) = new PageRowRDD(rdd)(this)

  //these are the entry points of SpookyStuff starting from a common RDD of strings or maps
  implicit def stringRDDToPageRowRDD(rdd: RDD[String]): PageSchemaRDD = {
    val result = rdd.map{
      str => {
        var cells = Map[String,Any]()
        if (str!=null) cells = cells + ("_" -> str)

        new PageRow(cells)
      }
    }

    new PageSchemaRDD(result, columnNames = ListSet("_"), spooky = this)
  }

  implicit def schemaRDDToPageRowRDD(rdd: SchemaRDD): PageSchemaRDD = {

    val result = new SchemaRDDFunctions(rdd).toMapRDD.map{
      map => new PageRow(Option(map).getOrElse(Map()))
    }

    new PageSchemaRDD(result, columnNames = ListSet(rdd.schema.fieldNames: _*), spooky = this)
  }

  implicit def mapRDDToPageRowRDD[T <: Any](rdd: RDD[Map[String,T]]): PageSchemaRDD = {

    val jsonRDD = rdd.map(map => Utils.toJson(map))
    jsonRDD.persist()
    val schemaRDD = sqlContext.jsonRDD(jsonRDD)

    schemaRDDToPageRowRDD(schemaRDD)
  }
}