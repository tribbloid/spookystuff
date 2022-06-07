package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.conf.{Core, PluginRegistry, PluginSystem}
import com.tribbloids.spookystuff.utils.{SpookyUtils, TreeThrowable}
import com.tribbloids.spookystuff.utils.io.Progress
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.apache.spark.TaskContext

import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Try

/**
  * the only implementation should be manually cleaned By ActionLike, so don't set lifespan unless absolutely necessary
  */
class Session(
    val spooky: SpookyContext,
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM().forShipping
) extends LocalCleanable {

  spooky.spookyMetrics.sessionInitialized += 1
  val startTimeMillis: Long = new Date().getTime
  val backtrace: ArrayBuffer[Action] = ArrayBuffer()

  object SessionLog {

    lazy val level1: String = SpookyUtils.canonizeFileName(lifespan.registeredIDs.mkString("-"), noDash = true)

    lazy val level2: String = SpookyUtils.canonizeFileName(startTimeMillis.toString, noDash = true)

    lazy val dirPath: String = s"logs/$level1/$level2"
  }

  def taskContextOpt: Option[TaskContext] = lifespan.ctx.taskOpt

  lazy val progress: Progress = Progress()

  object Drivers extends PluginRegistry.Factory {

    type UB = PluginSystem.WithDriver
    implicit override lazy val ubEv: ClassTag[UB] = ClassTag(classOf[UB])

    override type Out[V <: UB] = V#Driver

    override def compute[V <: UB](v: V): Out[V] = {
      val plugin: V#Plugin = spooky.Plugins.apply(v)

      progress.ping()
      val result = plugin.driverFactory.dispatch(Session.this)
      progress.ping()

      spooky.getMetric(Core).driverDispatched.add(plugin.driverFactory.toString -> 1L)

      result
    }

    def releaseAll(): Unit = {
      val plugins = spooky.Plugins.cache.values.toList

      val wDrivers = plugins.collect {
        case p: PluginSystem.WithDriver#Plugin =>
          p
      }

      val trials = wDrivers.map { p =>
        Try {
          p.driverFactoryOpt.foreach { v =>
            v.release(Session.this)
            cache -= p.pluginSystem

            spooky.getMetric(Core).driverReleased.add(v.toString -> 1L)
          }
        }
      }

      require(cache.isEmpty, "cache not empty")

      TreeThrowable.&&&(trials)
    }

    /**
      * all drivers will be terminated, not released (as in cleanImpl) currently useless
      */
    def shutdownAll(): Unit = {

      ???
    }
  }

  def driverOf[V <: Drivers.UB](v: V): V#Driver = Drivers.apply(v)

  override def cleanImpl(): Unit = {
    Drivers.releaseAll()

    spooky.spookyMetrics.sessionReclaimed += 1
  }
}
