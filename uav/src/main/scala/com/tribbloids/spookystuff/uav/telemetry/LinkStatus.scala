package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.utils.IDMixin

/**
  * Created by peng on 24/02/17.
  * Only used in tests
  */
case class LinkStatus(
                       uav: UAV,
                       home: Location = UAVConf.DEFAULT_HOME_LOCATION,
                       currentLocation: Location = UAVConf.DEFAULT_HOME_LOCATION
                     ) extends Link with IDMixin {

  override def _id: Any = uav

  override val exclusiveURIs: Seq[String] = uav.uris

  override protected def _connect(): Unit = {}

  override protected def _disconnect(): Unit = {}

  override def coFactory(v: Link): Boolean = {
    v match {
      case LinkStatus(u2, _, _) => u2 == this.uav
      case _ => false
    }
  }
  //  override def coFactory(v: Link): Boolean = false

  override protected def _getHome: Location = home

  override protected def _getCurrentLocation: Location = currentLocation

  override def status(expireAfter: Long): LinkStatus = this

  override val synch: SynchronousAPI = new SynchronousAPI {

    override def goto(location: Location) = ???

    override def testMove = ???

    override def clearanceAlt(alt: Double) = ???
  }
}
