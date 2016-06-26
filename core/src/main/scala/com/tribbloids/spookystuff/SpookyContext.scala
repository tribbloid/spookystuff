package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.DriverFactories
import com.tribbloids.spookystuff.dsl.DriverFactories.PhantomJS
import com.tribbloids.spookystuff.extractors.TypeTag
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.{HDFSResolver, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, TypeUtils}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag

case class SpookyContext private (
                                   @transient sqlContext: SQLContext, //can't be used on executors
                                   @transient private var _conf: SpookyConf, //can only be used on executors after broadcast
                                   var metrics: Metrics //accumulators cannot be broadcasted,
                                 ) {

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

  val browsersExist = deployPhantomJS()
  def sparkContext = this.sqlContext.sparkContext

  @volatile var broadcastedEffectiveConf = sqlContext.sparkContext.broadcast(_conf)

  def conf = if (_conf == null) broadcastedEffectiveConf.value
  else _conf

  def conf_=(conf: SpookyConf): Unit = {
    _conf = conf.importFrom(sqlContext.sparkContext)
    rebroadcast()
  }

  def rebroadcast(): Unit ={
    try {
      broadcastedEffectiveConf.destroy()
    }
    catch {
      case e: Throwable =>
    }
    broadcastedEffectiveConf = sqlContext.sparkContext.broadcast(_conf)
  }

  val broadcastedHadoopConf =
    if (sqlContext!=null) {
      sqlContext.sparkContext.broadcast(
        new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration)
      )
    }
    else null

  @transient lazy val hadoopConf: Configuration = broadcastedHadoopConf.value.value
  @transient lazy val resolver = HDFSResolver(hadoopConf)

  //TODO: use reflection to zero, and change var to val
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

  def create(df: DataFrame): FetchedDataset = this.dsl.dataFrameToPageRowRDD(df)
  def create[T: TypeTag](rdd: RDD[T]): FetchedDataset = this.dsl.rddToPageRowRDD(rdd)

  import TypeUtils.Implicits._

  def create[T: TypeTag](
                          seq: TraversableOnce[T]
                        ): FetchedDataset = {

    implicit val ctg = implicitly[TypeTag[T]].toClassTag
    this.dsl.rddToPageRowRDD(this.sqlContext.sparkContext.parallelize(seq.toSeq))
  }

  def create[T: TypeTag](
                          seq: TraversableOnce[T],
                          numSlices: Int
                        ): FetchedDataset = {

    implicit val ctg = implicitly[TypeTag[T]].toClassTag
    this.dsl.rddToPageRowRDD(this.sqlContext.sparkContext.parallelize(seq.toSeq, numSlices))
  }

  lazy val blankSelfRDD = sparkContext.parallelize(Seq(SquashedFetchedRow.blank))

  def blankFetchedDataset = this.create(blankSelfRDD)

  def createBeaconRDD[K: ClassTag,V: ClassTag](
                                                ref: RDD[_],
                                                partitionerFactory: RDD[_] => Partitioner = conf.defaultPartitionerFactory
                                              ): RDD[(K,V)] = {
    sparkContext
      .emptyRDD[(K,V)]
      .partitionBy(partitionerFactory(ref))
      .persist(StorageLevel.MEMORY_ONLY)
  }

  object dsl extends Serializable {

    import com.tribbloids.spookystuff.utils.ImplicitUtils._

    implicit def dataFrameToPageRowRDD(df: DataFrame): FetchedDataset = {
      val self: SquashedFetchedRDD = new DataFrameView(df)
        .toMapRDD(false)
        .map {
          map =>
            SquashedFetchedRow(
              Option(ListMap(map.toSeq: _*))
                .getOrElse(ListMap())
                .map(tuple => (Field(tuple._1), tuple._2))
            )
        }
      val fields = df.schema.fields.map {
        sf =>
          Field(sf.name) -> sf.dataType
      }
      new FetchedDataset(
        self,
        fieldMap = ListMap(fields: _*),
        spooky = getSpookyForInput
      )
    }

    //every input or noInput will generate a new metrics
    implicit def rddToPageRowRDD[T: TypeTag](rdd: RDD[T]): FetchedDataset = {

      val ttg = implicitly[TypeTag[T]]

      rdd match {
        // RDD[Map] => JSON => DF => ..
        case _ if ttg.tpe <:< TypeUtils.typeOf[Map[_,_]] =>
          //        classOf[Map[_,_]].isAssignableFrom(classTag[T].runtimeClass) => //use classOf everywhere?
          val canonRdd = rdd.map(
            map =>map.asInstanceOf[Map[_,_]].canonizeKeysToColumnNames
          )

          val jsonRDD = canonRdd.map(
            map =>
              Utils.toJson(map)
          )
          val dataFrame = sqlContext.read.json(jsonRDD)
          dataFrameToPageRowRDD(dataFrame)

        // RDD[SquashedFetchedRow] => ..
        //discard schema
        case _ if ttg.tpe <:< TypeUtils.typeOf[SquashedFetchedRow] =>
          //        case _ if classOf[SquashedFetchedRow] == classTag[T].runtimeClass =>
          val self = rdd.asInstanceOf[SquashedFetchedRDD]
          new FetchedDataset(
            self,
            fieldMap = ListMap(),
            spooky = getSpookyForInput
          )

        // RDD[T] => RDD('_ -> T) => ...
        case _ =>
          val self = rdd.map{
            str =>
              var cells = ListMap[Field,Any]()
              if (str!=null) cells = cells + (Field("_") -> str)

              SquashedFetchedRow(cells)
          }
          new FetchedDataset(
            self,
            fieldMap = ListMap(Field("_") -> TypeUtils.catalystTypeOrDefault()(ttg)),
            spooky = getSpookyForInput
          )
      }
    }
  }
}