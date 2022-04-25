package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf.{Core, PluginRegistry, PluginSystem, Python}
import com.tribbloids.spookystuff.utils.TreeThrowable
import com.tribbloids.spookystuff.utils.lifespan.Lifespan

import scala.reflect.ClassTag
import scala.util.Try

/**
  * the only implementation
  * should be manually cleaned By ActionLike, so don't set lifespan unless absolutely necessary
  */
class Session(
    override val spooky: SpookyContext,
    override val _lifespan: Lifespan = new Lifespan.JVM()
) extends AbstractSession(spooky) {

  object Drivers extends PluginRegistry.Factory {

    type UB = PluginSystem.WithDrivers
    override implicit lazy val ubEv: ClassTag[UB] = ClassTag(classOf[UB])

    override type Out[V <: UB] = V#Driver

    override def compute[V <: UB](v: V): Out[V] = {
      val plugin: V#Plugin = spooky.Plugins.apply(v)

      val result = plugin.driverFactory.dispatch(Session.this)

      spooky.getMetric(Core).driverDispatched.add(plugin.driverFactory.toString -> 1L)

      result
    }
  }

  def driverOf[V <: Drivers.UB](v: V): V#Driver = Drivers.apply(v)

  def pythonDriver: PythonDriver = {

    driverOf(Python)
  }

  override def cleanImpl(): Unit = {
    val plugins = spooky.Plugins.cache.values.toList

    val withDrivers = plugins.collect {
      case p: PluginSystem.WithDrivers#PluginLike =>
        p
    }

    val trials = withDrivers.map { p =>
      Try {
        p.driverFactoryOpt.foreach { v =>
          v.release(this)

          spooky.getMetric(Core).driverReleased.add(v.toString -> 1L)
        }
      }
    }

    TreeThrowable.&&&(trials)

//    Option(spooky.spookyConf.webDriverFactory).foreach { factory =>
//      factory.release(this)
//      spooky.spookyMetrics.webDriverReleased += 1
//    }
//    Option(spooky.spookyConf.pythonDriverFactory).foreach { factory =>
//      factory.release(this)
//      spooky.spookyMetrics.pythonDriverReleased += 1
//    }
    spooky.spookyMetrics.sessionReclaimed += 1
  }
}
