package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.conf.{AbstractConf, DirConf, SpookyConf, Submodules}
import com.tribbloids.spookystuff.metrics.{Metrics, SpookyMetrics}
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.io.HDFSResolver
import com.tribbloids.spookystuff.utils.serialization.SerDeOverride
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import com.tribbloids.spookystuff.utils.{ShippingMarks, TreeThrowable}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.dsl.utils.messaging.MessageWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag

case class SpookyContext(
    @transient sqlContext: SQLContext, //can't be used on executors, TODO: change to SparkSession
    // TODO: change to Option or SparkContext
    var _configurations: Submodules[AbstractConf] = Submodules(), //always broadcasted
    _metrics: Submodules[Metrics] = Submodules() //accumulators cannot be broadcasted,
) extends ShippingMarks {

  import sqlContext.sparkSession.implicits._

  def this(
      sqlContext: SQLContext,
      conf: SpookyConf
  ) {
    this(sqlContext, _configurations = Submodules(conf))
  }

  def this(sqlContext: SQLContext) {
    this(sqlContext, new SpookyConf())
  }

  def this(conf: SparkConf) {
    this(SparkSession.builder().config(conf).getOrCreate().sqlContext)
  }

  def this(sc: SparkContext) {
    this(sc.getConf)
  }

  {
    SpookyConf
    DirConf
    SpookyMetrics

    _configurations.buildAll[AbstractConf]()
    _metrics.buildAll[Metrics]()

    try {
      deployDrivers()
    } catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).error("Driver deployment fail on SpookyContext initialization", e)
    }
  }

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  def sparkContext: SparkContext = this.sqlContext.sparkContext

  def configurations: Submodules[AbstractConf] = {
    if (isShipped) {
      broadcastedConfigurations.value
    } else {
      _configurations.buildAll[AbstractConf]()

      _configurations = _configurations.transform { conf =>
        conf.importFrom(sparkContext.getConf)
      }
      _configurations
    }
  }

  def getConf[T <: AbstractConf: Submodules.Builder]: T = {
    configurations.getOrBuild[T]
  }

  def spookyConf: SpookyConf = getConf[SpookyConf]
  def dirConf: DirConf = getConf[DirConf]

  /**
    * can only be used on driver
    */
  def setConf[T <: AbstractConf](v: T)(implicit ev: Submodules.Builder[T]): Unit = {
    requireNotShipped()
    implicit val ctg: ClassTag[T] = ev.ctg
    _configurations.transform {
      case _: T  => v
      case v @ _ => v
    }
  }

  def spookyConf_=(v: SpookyConf): Unit = {
    setConf(v)
  }
  def dirConf_=(v: DirConf): Unit = {
    setConf(v)
  }

  val broadcastedHadoopConf: Broadcast[SerDeOverride[Configuration]] = {
    sqlContext.sparkContext.broadcast(
      SerDeOverride(this.sqlContext.sparkContext.hadoopConfiguration)
    )
  }
  def hadoopConf: SerDeOverride[Configuration] = broadcastedHadoopConf.value

  def pathResolver: HDFSResolver = HDFSResolver(hadoopConf)

  var broadcastedConfigurations: Broadcast[Submodules[AbstractConf]] = {
    sqlContext.sparkContext.broadcast(
      this.configurations
    )
  }

  def rebroadcast(): Unit = {
    requireNotShipped()
    scala.util.Try {
      broadcastedConfigurations.destroy()
    }
    broadcastedConfigurations = {
      sqlContext.sparkContext.broadcast(
        this.configurations
      )
    }
  }

  // may take a long time then fail, only attempted once
  def deployDrivers(): Unit = {
    val trials = spookyConf.driverFactories
      .map { v =>
        scala.util.Try {
          v.deployGlobally(this)
        }
      }
    TreeThrowable.&&&(trials)
  }

  def metrics: Submodules[Metrics] = {
    _metrics.buildAll[Metrics]()
    _metrics
  }

  def getMetrics[T <: Metrics: Submodules.Builder]: T = metrics.getOrBuild[T]

  def spookyMetrics: SpookyMetrics = getMetrics[SpookyMetrics]

  def zeroMetrics(): SpookyContext = {
    metrics.foreach {
      _.resetAll()
    }
    this
  }

  def getSpookyForRDD: SpookyContext = {
    if (spookyConf.shareMetrics) this
    else {
      rebroadcast()
      val result = this.copy(
        _configurations = this.configurations,
        _metrics = Submodules()
      )
      result
    }
  }

  def create(df: DataFrame): FetchedDataset = this.dsl.dataFrameToPageRowRDD(df)
  def create[T: TypeTag](rdd: RDD[T]): FetchedDataset = this.dsl.rddToFetchedDataset(rdd)

  //TODO: merge after 2.0.x
  def create[T: TypeTag](
      seq: TraversableOnce[T]
  ): FetchedDataset = {

    implicit val ctg: ClassTag[T] = ScalaType.fromTypeTag[T].asClassTag
    this.dsl.rddToFetchedDataset(this.sqlContext.sparkContext.parallelize(seq.toSeq))
  }
  def create[T: TypeTag](
      seq: TraversableOnce[T],
      numSlices: Int
  ): FetchedDataset = {

    implicit val ctg: ClassTag[T] = ScalaType.fromTypeTag[T].asClassTag
    this.dsl.rddToFetchedDataset(this.sqlContext.sparkContext.parallelize(seq.toSeq, numSlices))
  }

  def withSession[T](fn: Session => T): T = {

    val session = new Session(this)

    try {
      fn(session)
    } finally {
      session.tryClean()
    }
  }

  lazy val _blankSelfRDD: RDD[SquashedFetchedRow] = sparkContext.parallelize(Seq(SquashedFetchedRow.blank))

  def createBlank: FetchedDataset = this.create(_blankSelfRDD)

  object dsl extends Serializable {

    import com.tribbloids.spookystuff.utils.SpookyViews._

    implicit def dataFrameToPageRowRDD(df: DataFrame): FetchedDataset = {
      val mapRDD = new DataFrameView(df)
        .toMapRDD()

      val self: SquashedFetchedRDD = mapRDD
        .map { map =>
          SquashedFetchedRow(
            Option(ListMap(map.toSeq: _*))
              .getOrElse(ListMap())
              .map(tuple => (Field(tuple._1), tuple._2))
          )
        }
      val fields = df.schema.fields.map { sf =>
        Field(sf.name) -> sf.dataType
      }
      new FetchedDataset(
        self,
        fieldMap = ListMap(fields: _*),
        spooky = getSpookyForRDD
      )
    }

    //every input or noInput will generate a new metrics
    implicit def rddToFetchedDataset[T: TypeTag](rdd: RDD[T]): FetchedDataset = {

      val ttg = implicitly[TypeTag[T]]

      rdd match {
        // RDD[Map] => JSON => DF => ..
        case _ if ttg.tpe <:< typeOf[Map[_, _]] =>
          //        classOf[Map[_,_]].isAssignableFrom(classTag[T].runtimeClass) => //use classOf everywhere?
          val canonRdd = rdd.map(
            map => map.asInstanceOf[Map[_, _]].canonizeKeysToColumnNames
          )

          val jsonDS = sqlContext.createDataset[String](
            canonRdd.map(
              map => MessageWriter(map).compactJSON
            ))
          val dataFrame = sqlContext.read.json(jsonDS)
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
          val self = rdd.map { str =>
            var cells = ListMap[Field, Any]()
            if (str != null) cells = cells + (Field("_") -> str)

            SquashedFetchedRow(cells)
          }
          new FetchedDataset(
            self,
            fieldMap = ListMap(Field("_") -> ScalaType.fromTypeTag(ttg).asCatalystType),
            spooky = getSpookyForRDD
          )
      }
    }
  }
}
