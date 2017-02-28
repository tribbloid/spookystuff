package com.tribbloids.spookystuff.uav.sim

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.LLA
import com.tribbloids.spookystuff.uav.spatial.LLA.V

import scala.util.Random

/**
  * Created by peng on 01/10/16.
  */
//object SimFixture {
//
//  def launch(session: AbstractSession): APMSim = {
//    val sim = APMSim.next
//    sim.Py(session)
//    sim
//  }
//}

trait SimFactory extends Serializable {
  def getNext: APMSim
}

trait SimQuadFactory extends SimFactory {

  def getHomeLLA: V

  def getHomeStr: String = {
    val coordinate = getHomeLLA.productIterator.toSeq
    val yaw = Random.nextDouble() * 360.0
    (coordinate ++ Seq(yaw)).mkString(",")
  }

  def getNext: APMSim = {
    APMSim.next(
      extraArgs = Seq(
        "--model", "quad",
        "--gimbal",
        "--home", getHomeStr
      )
    )
  }
}

object DefaultSimFactory extends SimQuadFactory {

  val HOME_LLA = UAVConf.HOME_LOCATION.getCoordinate(LLA).get
  def getHomeLLA: V = {
      val lat = HOME_LLA.lat + (Random.nextDouble() - 0.5)*0.001
      val lon = HOME_LLA.lon + (Random.nextDouble() - 0.5)*0.001
      LLA(lat, lon, HOME_LLA.alt)
  }
}