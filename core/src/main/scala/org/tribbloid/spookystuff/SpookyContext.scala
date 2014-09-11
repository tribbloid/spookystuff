package org.tribbloid.spookystuff

import org.apache.spark.{SparkConf, SparkContext, SerializableWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.tribbloid.spookystuff.entity.{Page, PageRow}
import org.tribbloid.spookystuff.factory.driver.{DriverFactory, NaiveDriverFactory}
import org.tribbloid.spookystuff.operator.ExtractUrlEncodingPath
import org.tribbloid.spookystuff.sparkbinding.{PageRowRDDFunctions, StringRDDFunctions}

import scala.collection.immutable.ListMap

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

                      var autoSavePage: Boolean = true,
                      var autoSaveOutput: Boolean = false,
                      var pageSaveRoot: String = "s3n://spOOky/page/",
                      var pageSaveExtract: Page => String = ExtractUrlEncodingPath,

                      var errorDumpRoot: String = "s3n://spOOky/error/",
                      var localErrorDumpRoot: String = "temp/error/"
                      )
extends Serializable {

  def this(conf: SparkConf) {
    this(new SQLContext(new SparkContext(conf)))
  }

  def pagePath(
                page: Page,
                root: String = pageSaveRoot,
                select: Page => String = pageSaveExtract
                ): String = {

    if (!root.endsWith("/")) root + "/" + pageSaveExtract(page)
    else root + pageSaveExtract(page)
  }

  def errorDumpPath(
                     page: Page
                     ) = pagePath(page, errorDumpRoot)

  def localErrorDumpPath(
                          page: Page
                          ) = pagePath(page, localErrorDumpRoot)

  val hConfWrapper =  new SerializableWritable(this.sql.sparkContext.hadoopConfiguration)

  def hConf = hConfWrapper.value

  implicit def stringRDDToItsFunctions(rdd: RDD[String]) = new StringRDDFunctions(rdd)

  implicit def pageRowRDDToItsFunctions[T <% RDD[PageRow]](rdd: T) = new PageRowRDDFunctions(rdd)(this)

  //these are the entry points of SpookyStuff starting from a common RDD of strings or maps
  val noInput: RDD[PageRow] = this.sql.sparkContext.parallelize(Seq(PageRow()))

//  implicit def nullRDDToPageRowRDD(rdd: RDD[scala.Null]): RDD[PageRow] = rdd.map {
//    str => PageRow()
//  }

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