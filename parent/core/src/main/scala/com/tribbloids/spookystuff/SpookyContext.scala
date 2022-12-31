package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.conf._
import com.tribbloids.spookystuff.metrics.SpookyMetrics
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.io.HDFSResolver
import com.tribbloids.spookystuff.utils.serialization.SerDeOverride
import com.tribbloids.spookystuff.utils.{ShippingMarks, TreeThrowable}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import com.tribbloids.spookystuff.relay.io.Encoder
import org.apache.spark.ml.dsl.utils.refl.TypeMagnet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

object SpookyContext {

  def apply(
      sqlContext: SQLContext,
      conf: PluginSystem#MutableConfLike*
  ): SpookyContext = {
    val result = SpookyContext(sqlContext)
    result.setConf(conf: _*)
    result
  }

  implicit def toFetchedDS(spooky: SpookyContext): FetchedDataset = spooky.createBlank
}

case class SpookyContext(
    @transient sqlContext: SQLContext // can't be used on executors, TODO: change to SparkSession
) extends ShippingMarks {

  // can be shipped to executors to determine behaviours of actions
  // features can be configured in-place without affecting metrics
  // right before the shipping (implemented as serialisation hook),
  // all enabled features that are not configured will be initialised with default value

  object Plugins extends PluginRegistry.Factory {

    type UB = PluginSystem
    implicit override lazy val ubEv: ClassTag[UB] = ClassTag(classOf[UB])

    override type Out[T <: PluginSystem] = T#Plugin
    override def compute[T <: PluginSystem](v: T): v.Plugin = {
      requireNotShipped()
      val result = v.default(SpookyContext.this)
      result
    }

    def deployAll(): Unit = {
      createEnabled()
      val trials = cache.values.toList.map { plugin =>
        Try(plugin.deploy())
      }
      TreeThrowable.&&&(trials)
    }

    def resetAll(): Unit = {
      Plugins.cache.values.foreach { ff =>
        ff.reset()
      }
    }
  }

  def getPlugin[T <: PluginSystem](v: T): v.Plugin = Plugins.apply(v)
  def setPlugin(vs: PluginSystem#Plugin*): this.type = {
    requireNotShipped()

    vs.foreach { plugin =>
      Plugins.update(plugin.pluginSystem, plugin)
    }
    this
  }

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._
  import sqlContext.sparkSession.implicits._

  def sparkContext: SparkContext = this.sqlContext.sparkContext

  def getConf[T <: PluginSystem](v: T): v.Conf = getPlugin(v).getConf
  def setConf(vs: PluginSystem#MutableConfLike*): this.type = {
    requireNotShipped()

    val plugins = vs.map { conf =>
      val sys = conf.pluginSystem
      val old = getPlugin(sys)
      val neo: PluginSystem#Plugin = old.withConf(conf.asInstanceOf[sys.Conf])

      neo
    }

    setPlugin(plugins: _*)
  }

  def getMetric[T <: PluginSystem](v: T): v.Metrics = getPlugin(v).metrics

  def spookyConf: SpookyConf = getPlugin(Core).getConf
  def spookyConf_=(v: SpookyConf): Unit = {
    setConf(v)
  }
  def dirConf: Dir.Conf = getPlugin(Dir).getConf
  def dirConf_=(v: Dir.Conf): Unit = {
    setConf(v)
  }

  val hadoopConfBroadcast: Broadcast[SerDeOverride[Configuration]] = {
    sqlContext.sparkContext.broadcast(
      SerDeOverride(this.sqlContext.sparkContext.hadoopConfiguration)
    )
  }
  def hadoopConf: Configuration = hadoopConfBroadcast.value.value

  @transient lazy val pathResolver: HDFSResolver = HDFSResolver(() => hadoopConf)

  def spookyMetrics: SpookyMetrics = getPlugin(Core).metrics

  final override def clone: SpookyContext = {
    val result = SpookyContext(sqlContext)
    val plugins = Plugins.cache.values.toList.map(plugin => plugin.clone)
    result.setPlugin(plugins: _*)

    result
  }

  def forkForNewRDD: SpookyContext = {
    if (spookyConf.shareMetrics) {
      this // TODO: this doesn't fork configuration and may still cause interference
    } else {
      this.clone
    }
  }

  def create(df: DataFrame): FetchedDataset = this.dsl.dfToFetchedDS(df)
  def create[T: TypeTag](rdd: RDD[T]): FetchedDataset = this.dsl.rddToFetchedDS(rdd)

  // TODO: merge after 2.0.x
  def create[T: TypeTag](
      seq: TraversableOnce[T]
  ): FetchedDataset = {

    implicit val ctg: ClassTag[T] = TypeMagnet.FromTypeTag[T].asClassTag
    this.dsl.rddToFetchedDS(this.sqlContext.sparkContext.parallelize(seq.toSeq))
  }
  def create[T: TypeTag](
      seq: TraversableOnce[T],
      numSlices: Int
  ): FetchedDataset = {

    implicit val ctg: ClassTag[T] = TypeMagnet.FromTypeTag[T].asClassTag
    this.dsl.rddToFetchedDS(this.sqlContext.sparkContext.parallelize(seq.toSeq, numSlices))
  }

  def withSession[T](fn: Session => T): T = {

    val session = new Session(this)

    try {
      fn(session)
    } finally {
      session.tryClean()
    }
  }

  lazy val _blankRowRDD: RDD[SquashedFetchedRow] = sparkContext.parallelize(Seq(SquashedFetchedRow.blank))

  def createBlank: FetchedDataset = this.create(_blankRowRDD)

  object dsl extends Serializable {

    import com.tribbloids.spookystuff.utils.SpookyViews._

    implicit def dfToFetchedDS(df: DataFrame): FetchedDataset = {
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
        spooky = forkForNewRDD
      )
    }

    // every input or noInput will generate a new metrics
    implicit def rddToFetchedDS[T: TypeTag](rdd: RDD[T]): FetchedDataset = {

      val ttg = implicitly[TypeTag[T]]

      rdd match {
        // RDD[Map] => JSON => DF => ..
        case _ if ttg.tpe <:< typeOf[Map[_, _]] =>
          //        classOf[Map[_,_]].isAssignableFrom(classTag[T].runtimeClass) => //use classOf everywhere?
          val canonRdd = rdd.map(map => map.asInstanceOf[Map[_, _]].canonizeKeysToColumnNames)

          val jsonDS = sqlContext.createDataset[String](canonRdd.map(map => Encoder.forValue(map).compactJSON))
          val dataFrame = sqlContext.read.json(jsonDS)
          dfToFetchedDS(dataFrame)

        // RDD[SquashedFetchedRow] => ..
        // discard schema
        case _ if ttg.tpe <:< typeOf[SquashedFetchedRow] =>
          //        case _ if classOf[SquashedFetchedRow] == classTag[T].runtimeClass =>
          val self = rdd.asInstanceOf[SquashedFetchedRDD]
          new FetchedDataset(
            self,
            fieldMap = ListMap(),
            spooky = forkForNewRDD
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
            fieldMap = ListMap(Field("_") -> TypeMagnet.FromTypeTag(ttg).asCatalystType),
            spooky = forkForNewRDD
          )
      }
    }
  }
}
