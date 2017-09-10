package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.spatial.{Anchor, Location}
import com.tribbloids.spookystuff.uav.utils.UAVViews
import com.tribbloids.spookystuff.uav.{UAVConf, UAVMetrics}
import org.slf4j.LoggerFactory

/**
  * unless mixin, assume cost is 0
  */
trait HasCost {
  self: Action =>
}

trait UAVAction extends Action {
  {
    UAVConf
    UAVMetrics
  }
}

/**
  * inbound -> engage -> outbound
  */
trait UAVNavigation extends Interaction with UAVAction with HasCost {

  def _end: Location
  def _start: Location = _end

  def speedOpt: Option[Double] = None

  def replaceAnchor(fn: PartialFunction[Anchor, Anchor]): this.type = this

  override def exeNoOutput(session: Session): Unit = {

    val sv = this.getSessionView(session)
    sv.inbound()
    sv.engage()
    sv.outbound()
  }

  def getSessionView(session: Session) = new this.SessionView(session)

  implicit class SessionView(session: Session) extends UAVViews.SessionView(session) {

    lazy val _alt = uavConf.clearanceAltitudeMin
    /**
      * when enclosed in an export, may behave differently.
      */
    def inbound(): Unit = {

      LoggerFactory.getLogger(this.getClass)
        .info(s"climbing to ${_alt}")

      link.synch.clearanceAlt(_alt)
    }

    def engage(): Unit = {}

    def outbound(): Unit = {}
  }
}