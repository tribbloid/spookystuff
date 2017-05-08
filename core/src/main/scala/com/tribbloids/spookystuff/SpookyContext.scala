package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.{HDFSResolver, ScalaType, SerializationMarks, TreeException}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.dsl.utils.MessageView
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

case class SpookyContext private (
                                   @transient sqlContext: SQLContext, //can't be used on executors, TODO: change to Option or SparkContext
                                   private var _conf: SpookyConf, //can only be used on executors after broadcast
                                   metrics: SpookyMetrics //accumulators cannot be broadcasted,
                                 ) extends SerializationMarks {

  def this(
            sqlContext: SQLContext,
            conf: SpookyConf = new SpookyConf()
          ) {
    this(sqlContext, conf, SpookyMetrics())
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

  {
    try {
      deployDrivers()
    }
    catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).error("Driver deployment fail on SpookyContext initialization", e)
    }
  }

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  def sparkContext: SparkContext = this.sqlContext.sparkContext

  def conf: SpookyConf = {
    if (isShipped) {
      _conf
    }
    else {
      val sparkConf = sparkContext.getConf
      _conf.sparkConf = sparkConf
      _conf = _conf.effective
      _conf
    }
  }
  /**
    * can only be used on driver
    */
  def conf_= (conf: SpookyConf): Unit = {
    requireNotShipped()
    _conf = conf
  }

  def submodule[T <: ModuleConf: Submodules.Builder] = conf.submodule[T]

  val broadcastedHadoopConf: Broadcast[SerializableWritable[Configuration]] = sqlContext.sparkContext.broadcast(
    new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration)
  )
  def hadoopConf: Configuration = broadcastedHadoopConf.value.value

  def pathResolver = HDFSResolver(hadoopConf)

  //  private def resynch() = {
  //    _conf.sparkConf = sqlContext.sparkContext.getConf
  //    _conf = _conf.effective
  //  }
  def rebroadcast(): Unit = {
    //    requireNotShipped()
    //    scala.util.Try {
    //      broadcastedSpookyConf.destroy()
    //    }
    //    resynch()
    //    broadcastedSpookyConf = sqlContext.sparkContext.broadcast(_conf.effective)
  }

  // may take a long time then fail, only attempted once
  def deployDrivers(): Unit = {
    val trials = conf.driverFactories
      .map {
        v =>
          scala.util.Try {
            v.deploy(this)
          }
      }
    TreeException.&&&(trials)
  }

  def zeroMetrics(): SpookyContext ={
    metrics.zero()
    this
  }

  def getSpookyForRDD = {
    if (conf.shareMetrics) this
    else {
      rebroadcast()
      val result = this.copy(
        _conf = this._conf.effective,
        metrics = SpookyMetrics()
      )
      result
    }
  }

  def create(df: DataFrame): FetchedDataset = this.dsl.dataFrameToPageRowRDD(df)
  def create[T: TypeTag](rdd: RDD[T]): FetchedDataset = this.dsl.rddToPageRowRDD(rdd)

  //TODO: merge after 2.0.x
  def create[T: TypeTag](
                          seq: TraversableOnce[T]
                        ): FetchedDataset = {

    implicit val ctg = ScalaType.fromTypeTag[T].asClassTag
    this.dsl.rddToPageRowRDD(this.sqlContext.sparkContext.parallelize(seq.toSeq))
  }
  def create[T: TypeTag](
                          seq: TraversableOnce[T],
                          numSlices: Int
                        ): FetchedDataset = {

    implicit val ctg = ScalaType.fromTypeTag[T].asClassTag
    this.dsl.rddToPageRowRDD(this.sqlContext.sparkContext.parallelize(seq.toSeq, numSlices))
  }

  def withSession[T](fn: Session => T): T = {

    val session = new Session(this)

    try {
      fn(session)
    }
    finally {
      session.tryClean()
    }
  }

  lazy val _blankSelfRDD = sparkContext.parallelize(Seq(SquashedFetchedRow.blank))

  def createBlank = this.create(_blankSelfRDD)

  object dsl extends Serializable {

    import com.tribbloids.spookystuff.utils.SpookyViews._

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
        spooky = getSpookyForRDD
      )
    }

    //every input or noInput will generate a new metrics
    implicit def rddToPageRowRDD[T: TypeTag](rdd: RDD[T]): FetchedDataset = {

      val ttg = implicitly[TypeTag[T]]

      rdd match {
        // RDD[Map] => JSON => DF => ..
        case _ if ttg.tpe <:< typeOf[Map[_,_]] =>
          //        classOf[Map[_,_]].isAssignableFrom(classTag[T].runtimeClass) => //use classOf everywhere?
          val canonRdd = rdd.map(
            map =>map.asInstanceOf[Map[_,_]].canonizeKeysToColumnNames
          )

          val jsonRDD = canonRdd.map(
            map =>
              MessageView(map).compactJSON()
          )
          val dataFrame = sqlContext.read.json(jsonRDD)
          dataFrameToPageRowRDD(dataFrame)

        // RDD[SquashedFetchedRow] => ..
        //discard schema
        case _ if ttg.tpe <:< typeOf[SquashedFetchedRow] =>
          //        case _ if classOf[SquashedFetchedRow] == classTag[T].runtimeClass =>
          val self = rdd.asInstanceOf[SquashedFetchedRDD]
          new FetchedDataset(
            self,
            fieldMap = ListMap(),
            spooky = getSpookyForRDD
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
            fieldMap = ListMap(Field("_") -> ScalaType.fromTypeTag(ttg).reifyOrError),
            spooky = getSpookyForRDD
          )
      }
    }
  }
}