package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.PreDef
import com.tribbloids.spookystuff.conf._
import com.tribbloids.spookystuff.metrics.SpookyMetrics
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.relay.io.Encoder
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.utils.io.HDFSResolver
import com.tribbloids.spookystuff.utils.serialization.{NOTSerializable, SerializerOverride}
import com.tribbloids.spookystuff.utils.{ShippingMarks, SparkContextView, TreeThrowable}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.dsl.utils.refl.{ToCatalyst, TypeMagnet}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

object SpookyContext {

  def apply(
      sqlContext: SQLContext,
      conf: PluginSystem#ConfLike*
  ): SpookyContext = {
    val result = SpookyContext(sqlContext)
    result.setConf(conf: _*)
    result
  }

  implicit def asCoreAccessor(spookyContext: SpookyContext): spookyContext.Accessor[Core.type] = spookyContext(Core)

  implicit def asBlankFetchedDS(spooky: SpookyContext): FetchedDataset = spooky.createBlank

  implicit def asSparkContextView(spooky: SpookyContext): SparkContextView = SparkContextView(spooky.sparkContext)

  trait CanRunWith {

    type _WithCtx <: NOTSerializable // TODO: with AnyVal
    def _WithCtx: SpookyContext => _WithCtx

    // cached results will be dropped for being NOTSerializable
    @transient final lazy val withCtx: PreDef.Fn.Cached[SpookyContext, _WithCtx] = PreDef.Fn(_WithCtx).cachedBy()
  }

}

case class SpookyContext(
    @transient sqlContext: SQLContext // can't be used on executors, TODO: change to SparkSession
) extends ShippingMarks {

  // can be shipped to executors to determine behaviours of actions
  // features can be configured in-place without affecting metrics
  // right before the shipping (implemented as serialisation hook),
  // all enabled features that are not configured will be initialised with default value

  object Plugins extends PluginRegistry.Factory[PluginSystem] {

    type Out[T <: PluginSystem] = T#Plugin

    override def init: Dependent = new Dependent {

      def apply[T <: PluginSystem](arg: T): arg.Plugin = {
        requireNotShipped()
        val result = arg.default(SpookyContext.this)
        result
      }
    }

    def registered: List[PluginSystem#PluginLike] = this.lookup.values.toList.collect {
      case plugin: PluginSystem#PluginLike =>
        plugin
    }

    def deployAll(): Unit = {

      Try {
        registerEnabled()
        val trials = registered.map { v =>
          v.tryDeploy()
        }

        TreeThrowable.&&&(trials)
      }
    }

    def resetAll(): Unit = {
      registered.foreach { ff =>
        ff.reset()
      }
    }

  }

  def getPlugin[T <: PluginSystem](v: T): v.Plugin = Plugins.apply(v: v.type)
  def setPlugin(vs: PluginSystem#Plugin*): this.type = {
    // no deployement
    requireNotShipped()

    vs.foreach { plugin =>
      Plugins.lookup.updateOverride(plugin.pluginSystem, plugin)
    }

    this
  }

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._
  import sqlContext.sparkSession.implicits._

  def sparkContext: SparkContext = this.sqlContext.sparkContext

//  def getConf[T <: PluginSystem](v: T): v.Conf = getPlugin(v).getConf
  def setConf(vs: PluginSystem#ConfLike*): this.type = {
    requireNotShipped()

    val plugins = vs.map { conf =>
      val sys = conf.pluginSystem
      val old = getPlugin(sys)
      val neo: PluginSystem#Plugin = old.withConf(conf.asInstanceOf[sys.Conf])

      neo
    }

    setPlugin(plugins: _*)
  }

  case class Accessor[T <: PluginSystem](v: T) extends NOTSerializable {

    lazy val plugin: v.Plugin = getPlugin(v)

    def conf: v.Conf = plugin.getConf
    def conf_=(conf: v.Conf): SpookyContext.this.type = setConf(conf)
    def confUpdate(updater: v.Conf => v.Conf): SpookyContext.this.type = {
      val newConf = updater(conf)
      conf_=(newConf)
    }

    def metric: v.Metrics = plugin.metrics
  }
  def apply(v: PluginSystem): Accessor[v.type] = Accessor(v)

  def dirConf: Dir.Conf = getPlugin(Dir).getConf
  def dirConf_=(v: Dir.Conf): Unit = {
    setConf(v)
  }

  val hadoopConfBroadcast: Broadcast[SerializerOverride[Configuration]] = {
    sqlContext.sparkContext.broadcast(
      SerializerOverride(this.sqlContext.sparkContext.hadoopConfiguration)
    )
  }
  def hadoopConf: Configuration = hadoopConfBroadcast.value.value

  @transient lazy val pathResolver: HDFSResolver = HDFSResolver(() => hadoopConf)

  def spookyMetrics: SpookyMetrics = getPlugin(Core).metrics

  final override def clone: SpookyContext = {
    val result = SpookyContext(sqlContext)
    val plugins = Plugins.registered.map(plugin => plugin.clone)
    result.setPlugin(plugins: _*)

    result
  }

  def forkForNewRDD: SpookyContext = {
    if (this.conf.shareMetrics) {
      this // TODO: this doesn't fork configuration and may still cause interference
    } else {
      this.clone
    }
  }

  def create(df: DataFrame): FetchedDataset = this.dsl.dfToFetchedDS(df)
  def create[T: TypeTag](rdd: RDD[T]): FetchedDataset = this.dsl.rddToFetchedDS(rdd)

  // TODO: merge after 2.0.x
  def create[T: TypeTag](
      seq: IterableOnce[T]
  ): FetchedDataset = {

    implicit val ctg: ClassTag[T] = TypeMagnet.FromTypeTag[T].asClassTag
    this.dsl.rddToFetchedDS(this.sqlContext.sparkContext.parallelize(seq.toSeq))
  }
  def create[T: TypeTag](
      seq: IterableOnce[T],
      numSlices: Int
  ): FetchedDataset = {

    implicit val ctg: ClassTag[T] = TypeMagnet.FromTypeTag[T].asClassTag
    this.dsl.rddToFetchedDS(this.sqlContext.sparkContext.parallelize(seq.toSeq, numSlices))
  }

  def withSession[T](fn: Agent => T): T = {

    val session = new Agent(this)

    try {
      fn(session)
    } finally {
      session.tryClean()
    }
  }

  def createBlank: FetchedDataset = {

    lazy val _rdd: RDD[SquashedRow] = sparkContext.parallelize(Seq(FetchedRow.blank.squash))
    this.create(_rdd)
  }

  object dsl extends Serializable {

    import com.tribbloids.spookystuff.utils.SpookyViews._

    implicit def dfToFetchedDS(df: DataFrame): FetchedDataset = {
      val mapRDD = new DataFrameView(df)
        .toMapRDD()

      val self: SquashedRDD = mapRDD
        .map { map =>
          val listMap: ListMap[Field, Any] = Option(ListMap(map.toSeq: _*))
            .getOrElse(ListMap())
            .map { tuple =>
              (Field(tuple._1), tuple._2)
            }

          SquashedRow.ofData(
            DataRow(listMap).withEmptyScope
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

        // RDD[SquashedRow] => ..
        // discard schema
        case _ if ttg.tpe <:< typeOf[SquashedRow] =>
          //        case _ if classOf[SquashedRow] == classTag[T].runtimeClass =>
          val self = rdd.asInstanceOf[SquashedRDD]
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

            FetchedRow(DataRow(cells)).squash
          }
          new FetchedDataset(
            self,
            fieldMap = ListMap(Field("_") -> ToCatalyst(TypeMagnet.FromTypeTag(ttg)).asCatalystType),
            spooky = forkForNewRDD
          )
      }
    }
  }
}
