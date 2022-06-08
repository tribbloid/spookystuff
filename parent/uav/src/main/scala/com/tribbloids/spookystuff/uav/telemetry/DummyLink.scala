package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.point.Location
import com.tribbloids.spookystuff.uav.system.UAV

/**
  * Created by peng on 24/02/17.
  * Only used in tests
  * DON'T merge with UAVStatus!
  * despite similarity, all Links are subjected to resource control and automatic resource cleanup
  */
case class DummyLink(
    uav: UAV,
    override val home: Location = UAVConf.DEFAULT_HOME_LOCATION,
    currentLocation: Location = UAVConf.DEFAULT_HOME_LOCATION
) extends Link {

  override val resourceURIs: Set[String] = uav.uris.toSet

  override protected def _connect(): Unit = {
    Thread.sleep(1000) //simulate telemetry delay
  }

  override protected def _disconnect(): Unit = {}

  override def sameFactoryWith(v: Link): Boolean = {
    v match {
      case DummyLink(u2, _, _) => u2 == this.uav
      case _                   => false
    }
  }
  //  override def coFactory(v: Link): Boolean = false

  override protected def _getHome: Location = home

  override protected def _getCurrentLocation: Location = currentLocation

  override val synchAPI: SynchronousAPI = new SynchronousAPI {

    override def goto(location: Location) = ???

    override def testMove = ???

    override def clearanceAlt(alt: Double) = ???
  }
}
