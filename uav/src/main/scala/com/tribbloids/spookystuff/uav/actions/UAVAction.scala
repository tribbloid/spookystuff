package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.StartEndLocation
import com.tribbloids.spookystuff.uav.system.Drone
import com.tribbloids.spookystuff.uav.telemetry.Link

import scala.concurrent.duration.Duration
import scala.util.Random

trait UAVAction extends Action {

  /**
    * if left Nil will randomly choose any one from the fleet.
    * can be changed by GenPartitioner to enforce globally optimal execution.
    * if task already has a drone (TaskLocal) and its not in this list, will throw an error! GenPartitioner can detect this early
    */
  class SessionView(session: Session) {

    val mavConf: UAVConf = {
      session.spooky.conf.submodule[UAVConf]
    }

    val link: Link = {
      Link.trySelect(
        Random.shuffle(mavConf.dronesInFleet.toList),
        session
      )
        .get
    }
  }
}

/**
  * inbound -> engage -> outbound
  */
trait UAVNavigation extends Interaction with UAVAction with StartEndLocation {

  override def exeNoOutput(session: Session): Unit = {

    val sv = this.getSessionView(session)
    sv.inbound()
    sv.engage()
    sv.outbound()
  }

  def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends super.SessionView(session) {
    /**
      * when enclosed in an export, may behave differently.
      */
    def inbound(): Unit = {}

    def engage(): Unit = {}

    def outbound(): Unit = {}
  }
}