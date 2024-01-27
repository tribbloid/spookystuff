package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.metrics.AbstractMetrics
import com.tribbloids.spookystuff.session.DriverLike
import com.tribbloids.spookystuff.utils.BroadcastWrapper
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import org.apache.spark.SparkConf
import com.tribbloids.spookystuff.relay.MessageAPI

trait PluginSystem extends Serializable {

  {
    enableOnce
  }

  type Conf <: MutableConfLike

  trait HasOuter {
    def pluginSystem: PluginSystem.this.type = PluginSystem.this
  }

  /**
    * all subclasses have to define default() in their respective companion object.
    */
  trait MutableConfLike extends MessageAPI with HasOuter {

    def importFrom(sparkConf: SparkConf): Conf // read from Spark options & env vars

    final override def clone: Conf = importFrom(PluginSystem.emptySparkConf)
  }

  type Metrics <: AbstractMetrics

  type Plugin <: PluginLike

  trait PluginLike extends Cleanable with HasOuter {

    val spooky: SpookyContext

    @transient def effectiveConf: Conf

    /**
      * only swap out configuration do not replace anything else
      */
    def withEffectiveConf(conf: Conf): Plugin

    final def withConf(conf: Conf): Plugin = {

      val v = conf.importFrom(spooky.sparkContext.getConf)
      withEffectiveConf(v)
    }

    val confBroadcastW: BroadcastWrapper[Conf] = BroadcastWrapper(effectiveConf)(spooky.sparkContext)

    def getConf: Conf = confBroadcastW.value

    def metrics: Metrics

    final def reset(): this.type = {
      metrics.resetAll()
      this
    }

    def deploy(): Unit = {
      confBroadcastW.rebroadcast()
    }

    // end of definitions

    final override def clone: Plugin = {
      default(spooky).withEffectiveConf(getConf)
    }

    /**
      * can only be called once
      */
    override protected def cleanImpl(): Unit = {
      confBroadcastW.clean(true)
    }
  }

  def default(spooky: SpookyContext): Plugin

  final def init(spooky: SpookyContext, conf: Conf): Plugin = {
    default(spooky).withConf(conf)
  }

  lazy val enableOnce: Unit = PluginRegistry.enable(this)
}

object PluginSystem {

  lazy val emptySparkConf: SparkConf = new SparkConf(false)

  trait HasDriver extends PluginSystem {

    type Driver <: DriverLike

    trait PluginLike extends super.PluginLike {

      def driverFactory: DriverFactory[Driver]

      def driverFactoryOpt: Option[DriverFactory[Driver]] = Option(driverFactory)

      override def deploy(): Unit = {
        super.deploy()

        driverFactoryOpt.foreach(_.deployGlobally(spooky))
      }
    }

    override type Plugin <: PluginLike
  }
}
