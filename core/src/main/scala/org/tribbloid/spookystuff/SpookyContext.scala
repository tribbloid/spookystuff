package org.tribbloid.spookystuff

import org.apache.spark.{SparkConf, SparkContext, SerializableWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.openqa.selenium.remote.server.DriverFactory
import org.tribbloid.spookystuff.entity.{Page, PageRow}
import org.tribbloid.spookystuff.factory.NaiveDriverFactory
import org.tribbloid.spookystuff.operator.SelectUrlEncodingPath
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
                      @transient sql: SQLContext, //compulsory, many things are not possible without SQL
                      var driverFactory: DriverFactory = NaiveDriverFactory,

                      var autoSave: Boolean = true,
                      var saveRoot: String = "s3n://spooky-page/",
                      var saveSelect: Page => String = SelectUrlEncodingPath,

                      var errorDumpRoot: String = "s3n://spooky-error/",
                      var localErrorDumpRoot: String = "temp/spooky-error/"
                      )
  extends Serializable{

  def this(
            sc: SparkContext,
            driverFactory: DriverFactory = NaiveDriverFactory,

            autoSave: Boolean = true,
            saveRoot: String = "s3n://spooky-page/",
            saveSelect: Page => String = SelectUrlEncodingPath,

            errorDumpRoot: String = "s3n://spooky-error/",
            localErrorDumpRoot: String = "temp/spooky-error/"
            ) {

    this(new SQLContext(sc), driverFactory, autoSave, saveRoot, saveSelect, errorDumpRoot, localErrorDumpRoot)
  }

  def this(conf: SparkConf) {
    this(new SQLContext(new SparkContext(conf)))
  }

  def pagePath(
                page: Page,
                root: String = saveRoot,
                select: Page => String = saveSelect
                ): String = {

    if (!root.endsWith("/")) root + "/" + saveSelect(page)
    else root + saveSelect(page)
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
  val empty: RDD[PageRow] = this.sql.sparkContext.parallelize(Seq(PageRow()))

  implicit def nullRDDToPageRowRDD(rdd: RDD[scala.Null]): RDD[PageRow] = rdd.map {
    str => PageRow()
  }

  implicit def stringRDDToPageRowRDD(rdd: RDD[String]): RDD[PageRow] = rdd.map{
    str => {
      var cells = ListMap[String,Any]()
      if (str!=null) cells = cells + ("_" -> str)

      new PageRow(cells)
    }
  }

  implicit def mapRDDToPageRowRDD[T <: Map[String,Any]](rdd: RDD[T]) = rdd.map{
    map => {
      var cells = ListMap[String,Any]()
      if (map!=null) cells = cells ++ map

      new PageRow(cells)
    }
  }
}