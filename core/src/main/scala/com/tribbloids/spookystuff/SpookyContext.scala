package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.DriverFactories.PhantomJS
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory
import com.tribbloids.spookystuff.dsl.DriverFactories
import com.tribbloids.spookystuff.entity.{Key, KeyLike, PageRow}
import com.tribbloids.spookystuff.sparkbinding.{DataFrameView, PageRowRDD}
import com.tribbloids.spookystuff.utils.{Views, Utils}

import scala.collection.immutable.{ListMap, ListSet}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

case class SpookyContext private (
                                   @transient sqlContext: SQLContext, //can't be used on executors
                                   @transient private var _effectiveConf: SpookyConf, //can only be used on executors after broadcast
                                   var metrics: Metrics //accumulators cannot be broadcasted,
                                   ) {

  val browsersExist = deployPhantomJS()

  def this(
            sqlContext: SQLContext,
            spookyConf: SpookyConf = new SpookyConf()
            ) {
    this(sqlContext, spookyConf.importFrom(sqlContext.sparkContext), new Metrics())
  }

  def this(sqlContext: SQLContext) {
    this(sqlContext, new SpookyConf())
  }

  def this(sc: SparkContext) {
    this(new SQLContext(sc))
  }

  def this(conf: SparkConf) {
    this(new SparkContext(conf))
  }

  def sparkContext = this.sqlContext.sparkContext

  @volatile var broadcastedEffectiveConf = sqlContext.sparkContext.broadcast(_effectiveConf)

  def conf = if (_effectiveConf == null) broadcastedEffectiveConf.value
  else _effectiveConf

  def conf_=(conf: SpookyConf): Unit = {
    _effectiveConf = conf.importFrom(sqlContext.sparkContext)
    broadcast()
  }

  def broadcast(): Unit ={
    broadcastedEffectiveConf.destroy()
    broadcastedEffectiveConf = sqlContext.sparkContext.broadcast(_effectiveConf)
  }

  val broadcastedHadoopConf = if (sqlContext!=null) sqlContext.sparkContext.broadcast(new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration))
  else null

  def hadoopConf: Configuration = broadcastedHadoopConf.value.value

  def zeroMetrics(): SpookyContext ={
    metrics = new Metrics()
    this
  }

  def getContextForNewInput = if (conf.shareMetrics) this
  else this.copy(metrics = new Metrics())

  private def deployPhantomJS(): Boolean = {
    val sc = sqlContext.sparkContext
    val phantomJSUrlOption = DriverFactories.PhantomJS.pathOptionFromEnv

    val effectiveURL = if (phantomJSUrlOption.isEmpty || conf.alwaysDownloadBrowserRemotely) PhantomJS.remotePhantomJSURL
    else phantomJSUrlOption.get

    try {
      LoggerFactory.getLogger(this.getClass).info(s"Deploying PhantomJS from $effectiveURL ...")
      sc.addFile(effectiveURL) //this only adds file into http server, executors doesn't necessarily download it.
      LoggerFactory.getLogger(this.getClass).info(s"Finished deploying PhantomJS from $effectiveURL")
      return true
    }
    catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).error(s"FAILED to deploy PhantomJS from $effectiveURL", e)
        return false
    }

    true
  }

  def create(df: DataFrame): PageRowRDD = this.dsl.dataFrameToPageRowRDD(df)
  def create[T: ClassTag](rdd: RDD[T]): PageRowRDD = this.dsl.rddToPageRowRDD(rdd)

  def create[T: ClassTag](seq: TraversableOnce[T]): PageRowRDD = this.dsl.rddToPageRowRDD(this.sqlContext.sparkContext.parallelize(seq.toSeq))
  def create[T: ClassTag](seq: TraversableOnce[T], numSlices: Int): PageRowRDD = this.dsl.rddToPageRowRDD(this.sqlContext.sparkContext.parallelize(seq.toSeq, numSlices))

  object dsl extends Serializable {

    implicit def dataFrameToPageRowRDD(df: DataFrame): PageRowRDD = {
      val self = new DataFrameView(df).toMapRDD.map {
        map =>
          PageRow(
            Option(ListMap(map.toSeq: _*))
              .getOrElse(ListMap())
              .map(tuple => (Key(tuple._1), tuple._2))
          )
      }
      new PageRowRDD(self, keys = ListSet(df.schema.fieldNames: _*).map(Key(_)), spooky = getContextForNewInput)
    }

    //every input or noInput will generate a new metrics
    implicit def rddToPageRowRDD[T: ClassTag](rdd: RDD[T]): PageRowRDD = {
      import Views._

      import scala.reflect._

      rdd match {
        case _ if classOf[Map[_,_]].isAssignableFrom(classTag[T].runtimeClass) => //use classOf everywhere?
          val canonRdd = rdd.map(
            map =>map.asInstanceOf[Map[_,_]].canonizeKeysToColumnNames
          )

          val jsonRDD = canonRdd.map(
            map =>
              Utils.toJson(map)
          )
          val dataFrame = sqlContext.jsonRDD(jsonRDD)
          val self = canonRdd.map(
            map =>
              PageRow(ListMap(map.map(tuple => (Key(tuple._1),tuple._2)).toSeq: _*), Array())
          )
          new PageRowRDD(self, keys = ListSet(dataFrame.schema.fieldNames: _*).map(Key(_)), spooky = getContextForNewInput)
        case _ =>
          val self = rdd.map{
            str =>
              var cells = ListMap[KeyLike,Any]()
              if (str!=null) cells = cells + (Key("_") -> str)

              PageRow(cells)
          }
          new PageRowRDD(self, keys = ListSet(Key("_")), spooky = getContextForNewInput)
      }
    }
  }
}