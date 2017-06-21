package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.system.UAV

/**
  * Created by peng on 24/02/17.
  * Only used in tests
  */
case class DummyLink(
                      uav: UAV
                    ) extends Link {

  override val exclusiveURIs: Seq[String] = uav.uris

  override protected def _connect(): Unit = {}

  override protected def _disconnect(): Unit = {}

  override def coFactory(v: Link): Boolean = {
    v match {
      case DummyLink(u2) => u2 == this.uav
      case _ => false
    }
  }
//  override def coFactory(v: Link): Boolean = false

  override protected def _getHome: Location = {UAVConf.DEFAULT_HOME_LOCATION}

  override protected def _getCurrentLocation: Location = {UAVConf.DEFAULT_HOME_LOCATION}

  override val synch: SynchronousAPI = new SynchronousAPI {

    override def move(location: Location) = {}

    override def testMove = {"dummy"}

    override def clearanceAlt(alt: Double) = {}
  }
}
