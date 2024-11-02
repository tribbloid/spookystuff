package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.spark.SparkContextView
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.TreeThrowable
import com.tribbloids.spookystuff.commons.serialization.{NOTSerializable, SerializerOverride}
import com.tribbloids.spookystuff.conf._
import com.tribbloids.spookystuff.io.HDFSResolver
import com.tribbloids.spookystuff.metrics.SpookyMetrics
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.ShippingMarks
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

object SpookyContext {

  def apply(
      sqlContext: SQLContext,
      conf: PluginSystem#ConfLike*
  ): SpookyContext = {
    val neo = SpookyContext(sqlContext.sparkSession)
    neo.setConf(conf: _*)
    neo
  }

  def apply(
      sparkSession: SparkSession,
      conf: PluginSystem#ConfLike*
  ): SpookyContext = {
    val neo = SpookyContext(sparkSession)
    neo.setConf(conf: _*)
    neo
  }

  implicit def asCoreAccessor(spookyContext: SpookyContext): spookyContext.Accessor[Core.type] = spookyContext(Core)

  implicit def asBlankFetchedDS(spooky: SpookyContext): FetchedDataset[Unit] = spooky.createBlank

  implicit def asSparkContextView(spooky: SpookyContext): SparkContextView = SparkContextView(spooky.sparkContext)

  trait CanRunWith {

    type _WithCtx <: NOTSerializable // TODO: with AnyVal
    def _WithCtx: SpookyContext => _WithCtx

    // cached results will be dropped for being NOTSerializable
    @transient final lazy val withCtx: SpookyContext :=> _WithCtx =
      :=>(_WithCtx).cached()
  }
}

case class SpookyContext(
    @transient sparkSession: SparkSession // can't be used on executors, TODO: change to SparkSession
) extends ShippingMarks {

  // can be shipped to executors to determine behaviours of actions
  // features can be configured in-place without affecting metrics
  // right before the shipping (implemented as serialisation hook),
  // all enabled features that are not configured will be initialised with default value

  object Plugins extends PluginRegistry.Factory[PluginSystem] {

    type Out[T <: PluginSystem] = T#Plugin

    override def init: Impl = new Impl {

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

  def getPlugin[T <: PluginSystem](v: T): v.Plugin = Plugins.cached.apply(v: v.type)
  def setPlugin(vs: PluginSystem#Plugin*): this.type = {
    // no deployement
    requireNotShipped()

    vs.foreach { plugin =>
      Plugins.lookup.update(plugin.pluginSystem, plugin)
    }

    this
  }

  def sqlContext: SQLContext = sparkSession.sqlContext

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

  def dirConf: Dir.Conf = apply(Dir).conf
  def dirConf_=(v: Dir.Conf): SpookyContext.this.type = {
    val dir = apply(Dir)
    dir.conf = v
  }

  val hadoopConfBroadcast: Broadcast[SerializerOverride[Configuration]] = {
    // TODO: this is still memory-consuming, can it be done only once?
    sqlContext.sparkContext.broadcast(
      SerializerOverride(this.sqlContext.sparkContext.hadoopConfiguration)
    )
  }
  def hadoopConf: Configuration = hadoopConfBroadcast.value.value

  @transient lazy val pathResolver: HDFSResolver = HDFSResolver(() => hadoopConf)

  def spookyMetrics: SpookyMetrics = getPlugin(Core).metrics

  final override def clone: SpookyContext = { // TODO: clean
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

  def create[T](rdd: RDD[T]): FetchedDataset[T] = fromRDD(rdd)
  def create[T](ds: Dataset[T]): FetchedDataset[T] = fromDataset(ds)

  // TODO: create Dataset directly
  def create[T: ClassTag](
      batch: IterableOnce[T]
  ): FetchedDataset[T] = {

    fromRDD(this.sqlContext.sparkContext.parallelize(batch.iterator.to(Seq)))
  }
  def create[T: ClassTag](
      batch: IterableOnce[T],
      numSlices: Int
  ): FetchedDataset[T] = {

    fromRDD(this.sqlContext.sparkContext.parallelize(batch.iterator.to(Seq), numSlices))
  }

  // every input or noInput will generate a new metrics
  def fromRDD[T](rdd: RDD[T]): FetchedDataset[T] = {

    //      val ttg = implicitly[TypeTag[T]]

    val self = rdd.map { data =>
      FetchedRow(data).squash
    }
    new FetchedDataset(
      self,
      spooky = forkForNewRDD
    )
  }

  def fromDataset[D](ds: Dataset[D]): FetchedDataset[D] = {

    fromRDD(ds.rdd)
  }

  def withSession[T](fn: Agent => T): T = {

    val session = new Agent(this)

    try {
      fn(session)
    } finally {
      session.tryClean()
    }
  }

  def createBlank: FetchedDataset[Unit] = {

    lazy val _rdd: RDD[Unit] = sparkContext.parallelize(Seq(()))
    this.create(_rdd)
  }

  object dsl extends Serializable {}
}
