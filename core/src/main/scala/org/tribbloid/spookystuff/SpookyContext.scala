package org.tribbloid.spookystuff

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SerializableWritable, SparkConf, SparkContext}
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.factory.driver.{DriverFactory, NaiveDriverFactory}
import org.tribbloid.spookystuff.operator._
import org.tribbloid.spookystuff.sparkbinding.{PageRowRDDFunctions, StringRDDFunctions}

import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration

import scala.concurrent.duration._

/**
 * Created by peng on 12/06/14.
 */
//class SpookyContext(val sc: SparkContext) {
//
//}

//will be shipped everywhere as implicit parameter

class SpookyContext (
                      @transient val sql: SQLContext, //compulsory, many things are not possible without SQL

                      var driverFactory: DriverFactory = NaiveDriverFactory(),

                      var PagePathLookup: Lookup[_] = VerbosePathLookup,
                      var PagePathExtract: Extract[_] = ExtractTimestamp,

                      var autoSave: Boolean = true,//slow, for debugging only
                      var autoSaveRoot: String = "s3n://spOOky/"+"page/",

                      var autoCache: Boolean = true,//slow, reduce bombarding highly aware sites
                      //                      var autoRestore: Boolean = true,//slow, reduce bombarding highly aware sites
                      var autoCacheRoot: String = "s3n://spOOky/"+"backup/",

                      var errorDump: Boolean = true,
                      var errorDumpRoot: String = "s3n://spOOky/"+"error/",
                      var localErrorDumpRoot: String = "file:///spOOky/"+"error/",

                      var resourceTimeout: Duration = 60.seconds
                      )
  extends Serializable {

  val hConfWrapper =  if (sql!=null) new SerializableWritable(this.sql.sparkContext.hadoopConfiguration)
  else null

  def hConf = hConfWrapper.value

  @transient lazy val noInput: RDD[PageRow] = this.sql.sparkContext.parallelize(Seq(PageRow()))

//  implicit def self: SpookyContext = this

  def setRoot(root: String): Unit = {

    autoSaveRoot = Utils.urlConcat(root, "page/")
    autoCacheRoot = Utils.urlConcat(root, "backup/")
    localErrorDumpRoot = Utils.urlConcat(root, "error/")
  }

  def this(conf: SparkConf) {
    this(new SQLContext(new SparkContext(conf)))
  }

  implicit def stringRDDToItsFunctions(rdd: RDD[String]) = new StringRDDFunctions(rdd)

  implicit def pageRowRDDToItsFunctions[T <% RDD[PageRow]](rdd: T) = new PageRowRDDFunctions(rdd)(this)

  //these are the entry points of SpookyStuff starting from a common RDD of strings or maps
  implicit def stringRDDToPageRowRDD(rdd: RDD[String]): RDD[PageRow] = rdd.map{
    str => {
      var cells = ListMap[String,Any]()
      if (str!=null) cells = cells + ("_" -> str)

      new PageRow(cells)
    }
  }

  implicit def mapRDDToPageRowRDD[T <: Map[String,Any]](rdd: RDD[T]): RDD[PageRow] = rdd.map{
    map => {
      var cells = ListMap[String,Any]()
      if (map!=null) cells = cells ++ map

      new PageRow(cells)
    }
  }
}