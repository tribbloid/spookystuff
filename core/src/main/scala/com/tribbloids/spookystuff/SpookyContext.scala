package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.DriverFactories
import com.tribbloids.spookystuff.dsl.DriverFactories.PhantomJS
import com.tribbloids.spookystuff.execution.DataFrameView
import com.tribbloids.spookystuff.rdd.PageRowRDD
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.{Utils, Views}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.immutable.{ListMap, ListSet}
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
    rebroadcast()
  }

  def rebroadcast(): Unit ={
    broadcastedEffectiveConf.destroy()
    broadcastedEffectiveConf = sqlContext.sparkContext.broadcast(_effectiveConf)
  }

  val broadcastedHadoopConf =
    if (sqlContext!=null) {
      sqlContext.sparkContext.broadcast(
        new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration)
      )
    }
    else null

  def hadoopConf: Configuration = broadcastedHadoopConf.value.value

  def zeroMetrics(): SpookyContext ={
    metrics = new Metrics()
    this
  }

  def getSpookyForInput = if (conf.shareMetrics) this
  else this.copy(metrics = new Metrics())

  //TODO: move to DriverFactory.initializeDeploy
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
      val self: RDD[SquashedPageRow] = new DataFrameView(df).toMapRDD.map {
        map =>
          SquashedPageRow(
            Option(ListMap(map.toSeq: _*))
              .getOrElse(ListMap())
              .map(tuple => (Field(tuple._1), tuple._2))
          )
      }
      new PageRowRDD(
        self,
        schema = ListSet(df.schema.fieldNames: _*).map(Field(_)),
        spooky = getSpookyForInput
      )
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
              SquashedPageRow(ListMap(map.map(tuple => (Field(tuple._1),tuple._2)).toSeq: _*))
          )
          new PageRowRDD(
            self,
            schema = ListSet(dataFrame.schema.fieldNames: _*).map(Field(_)),
            spooky = getSpookyForInput
          )
        case _ =>
          val self = rdd.map{
            str =>
              var cells = ListMap[Field,Any]()
              if (str!=null) cells = cells + (Field("_") -> str)

              SquashedPageRow(cells)
          }
          new PageRowRDD(
            self,
            schema = ListSet(Field("_")),
            spooky = getSpookyForInput
          )
      }
    }
  }

  def createBeaconRDD[K: ClassTag,V: ClassTag](
                                                ref: RDD[_],
                                                partitionerFactory: RDD[_] => Partitioner = conf.defaultPartitionerFactory
                                              ): RDD[(K,V)] = {
    sparkContext
      .emptyRDD[(K,V)]
      .partitionBy(partitionerFactory(ref))
      .persist(StorageLevel.MEMORY_ONLY)
  }
}