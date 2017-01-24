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
import scala.reflect.ClassTag

case class SpookyContext private (
                                   @transient sqlContext: SQLContext, //can't be used on executors, TODO: change to Option or SparkContext
                                   @transient private val spookyConf: SpookyConf, //can only be used on executors after broadcast
                                   metrics: SpookyMetrics //accumulators cannot be broadcasted,
                                 ) extends SerializationMarks {

  def this(
            sqlContext: SQLContext,
            conf: SpookyConf = new SpookyConf()
          ) {
    this(sqlContext, conf, new SpookyMetrics())
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
    rebroadcast()
  }

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  def sparkContext: SparkContext = this.sqlContext.sparkContext

  @volatile private var effectiveConf: SpookyConf = _
  private def setEffectiveConf(newConf: SpookyConf) = {
    effectiveConf = newConf.importFrom(sqlContext.sparkContext.getConf)
  }

  //maybe obsolete if not rebroadcasted TODO: package auto broadcast after shipping behaviour as Minimalistic API
  @volatile var broadcastedSpookyConf: Broadcast[SpookyConf] = _
  def conf: SpookyConf = {
    if (isShipped) broadcastedSpookyConf.value
    else {
      if (effectiveConf == null) setEffectiveConf(spookyConf)
      effectiveConf
    }
  }
  /**
    * can only be used on driver
    */
  def conf_= (conf: SpookyConf): Unit = {
    requireNotShipped()
    setEffectiveConf(conf)
    rebroadcast()
  }

  def submodule[T <: AbstractConf: ClassTag] = conf.submodule[T]

  @volatile var broadcastedHadoopConf: Broadcast[SerializableWritable[Configuration]] = _
  def hadoopConf: Configuration = broadcastedHadoopConf.value.value

  def resolver = HDFSResolver(hadoopConf)

  def rebroadcast(): Unit = {
    requireNotShipped()
    scala.util.Try {
      broadcastedSpookyConf.destroy()
    }
    scala.util.Try {
      broadcastedHadoopConf.destroy()
    }
    broadcastedSpookyConf = sqlContext.sparkContext.broadcast(effectiveConf)
    broadcastedHadoopConf = sqlContext.sparkContext.broadcast(
      new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration)
    )
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
      val result = this.copy(metrics = new SpookyMetrics())
      result.setEffectiveConf(this.effectiveConf)
      result
    }
  }

  def create(df: DataFrame): FetchedDataset = this.dsl.dataFrameToPageRowRDD(df)
  def create[T: TypeTag](rdd: RDD[T]): FetchedDataset = this.dsl.rddToPageRowRDD(rdd)

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