package org.tribbloid.spookystuff

import org.apache.spark.{SparkConf, SparkContext, SerializableWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.openqa.selenium.remote.server.DriverFactory
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.factory.NaiveDriverFactory
import org.tribbloid.spookystuff.sparkbinding.{PageRowRDDFunctions, StringRDDFunctions}

import scala.collection.immutable.ListMap

/**
 * Created by peng on 12/06/14.
 */
//class SpookyContext(val sc: SparkContext) {
//
//}

//will be shipped everywhere as implicit parameter
case class SpookyContext (
                           @transient sql: SQLContext, //compulsory, many things are not possible without SQL
                           var driverFactory: DriverFactory = NaiveDriverFactory
                           )
  extends Serializable{

  def this(sc: SparkContext) {
    this(new SQLContext(sc))
  }

  def this(conf: SparkConf) {
    this(new SQLContext(new SparkContext(conf)))
  }

  val hConfWrapper =  new SerializableWritable(sql.sparkContext.hadoopConfiguration)

  def hConf = hConfWrapper.value

  implicit def stringRDDToItsFunctions(rdd: RDD[String]) = new StringRDDFunctions(rdd)

  implicit def pageRowRDDToItsFunctions[T <% RDD[PageRow]](rdd: T) = new PageRowRDDFunctions(rdd)(this)

  //these are the entry points of SpookyStuff starting from a common RDD of strings or maps
  val Empty: RDD[PageRow] = sql.sparkContext.parallelize(Seq(PageRow()))

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