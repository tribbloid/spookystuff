package com.tribbloids.spookystuff.uav.sim

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.LLA

import scala.util.Random

/**
  * Created by peng on 01/10/16.
  */
trait SimFactory extends Serializable {

  def getNext: APMSim
}

trait MAVLinkSimFactory extends SimFactory {

  def getHomeLLA: LLA.C

  def model = "quad"

  def getNext: APMSim = {
    val homeStr: String = {
      val coordinate = getHomeLLA.productIterator.toSeq
      val yaw = Random.nextDouble() * 360.0
      (coordinate ++ Seq(yaw)).mkString(",")
    }

    APMSim.next(
      extraArgs = Seq(
        "--model", model,
        "--gimbal",
        "--home", homeStr
      )
    )
  }
}

case class APMQuadSimFactory(
                           dispersionLatLng: Double = 0.001
                         ) extends MAVLinkSimFactory {

  val homeCenter = UAVConf.DEFAULT_HOME_LOCATION.getCoordinate(LLA).get

  def getHomeLLA: LLA.C = {
    val lat = homeCenter.lat + (Random.nextDouble() - 0.5)*dispersionLatLng
    val lon = homeCenter.lon + (Random.nextDouble() - 0.5)*dispersionLatLng
    LLA(lat, lon, homeCenter.alt)
  }
}