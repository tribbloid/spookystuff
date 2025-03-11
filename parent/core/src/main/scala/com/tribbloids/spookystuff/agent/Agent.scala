package com.tribbloids.spookystuff.agent

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.commons.TreeThrowable
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.commons.lifespan.LocalCleanable
import com.tribbloids.spookystuff.conf.{Core, PluginRegistry, PluginSystem}
import com.tribbloids.spookystuff.io.Progress
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.TaskContext

import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * the only implementation should be manually cleaned By ActionLike, so don't set lifespan unless absolutely necessary
  */
class Agent(
    val spooky: SpookyContext,
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM().forShipping
) extends LocalCleanable {

  spooky.metrics.sessionInitialized += 1
  val startTimeMillis: Long = new Date().getTime
  val backtrace: ArrayBuffer[Action] = ArrayBuffer()

  object Log {

    lazy val level1: String = SpookyUtils.canonizeFileName(lifespan.registeredIDs.mkString("-"), noDash = true)

    lazy val level2: String = SpookyUtils.canonizeFileName(startTimeMillis.toString, noDash = true)

    lazy val dirPath: String = s"logs/$level1/$level2"
  }

  def taskContextOpt: Option[TaskContext] = lifespan.ctx.taskOpt

  lazy val progress: Progress = Progress()

  type Sys = PluginSystem.HasDriver

  object Drivers extends PluginRegistry.Factory[Sys] {

    override type Out[V <: Sys] = V#Driver

    override def init: Impl = new Impl {

      override def apply[V <: Sys](v: V): this.Out[V] = {
        val plugin: V#Plugin = spooky.Plugins.apply(v)

        progress.ping()
        val result = plugin.driverFactory.dispatch(Agent.this)
        progress.ping()

        spooky.apply(Core).metric.driverDispatched.add(plugin.driverFactory.toString -> 1L)

        result
      }
    }

    def releaseAll(): Unit = {
      val plugins = spooky.Plugins.lookup.values.toList

      val wDrivers = plugins.collect {
        case p: PluginSystem.HasDriver#Plugin =>
          p
      }

      val trials = wDrivers.map { (p: Sys#Plugin) =>
        Try {
          p.driverFactoryOpt.foreach { v =>
            v.release(Agent.this)
            cached.lookup remove p.pluginSystem

            spooky(Core).metric.driverReleased.add(v.toString -> 1L)
          }
        }
      }

      require(cached.lookup.isEmpty, "cache not empty")

      TreeThrowable.&&&(trials)
    }

    /**
      * all drivers will be terminated, not released (as in cleanImpl) currently useless
      */
//    def shutdownAll(): Unit = {
//
//      ???
//    }
  }

  def driverOf[V <: Sys](v: V): V#Driver = Drivers.apply(v)

  override def cleanImpl(): Unit = {
    Drivers.releaseAll()

    spooky.metrics.sessionReclaimed += 1
  }
}
