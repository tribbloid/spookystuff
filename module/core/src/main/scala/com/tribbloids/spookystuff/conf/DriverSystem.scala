package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.agent.DriverLike

import scala.util.Try

trait DriverSystem extends PluginSystem {

  type Driver <: DriverLike

  trait _PluginLike extends super.PluginLike {

    def driverFactory: DriverFactory[Driver]

    def driverFactoryOpt: Option[DriverFactory[Driver]] = Option(driverFactory)

    override def tryDeploy(): Try[Unit] = {
      super.tryDeploy().flatMap { _ =>
        Try {
          driverFactoryOpt.foreach(_.deployGlobally(spooky))
        }
      }
    }
  }

  override type Plugin <: _PluginLike
}
