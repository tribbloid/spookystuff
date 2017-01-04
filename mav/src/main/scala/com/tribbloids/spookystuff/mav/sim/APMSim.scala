package com.tribbloids.spookystuff.mav.sim

import com.tribbloids.spookystuff.caching
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.mav.actions.LocationGlobal
import com.tribbloids.spookystuff.session.LocalCleanable
import com.tribbloids.spookystuff.session.python.{CaseInstanceRef, SingletonRef}

import scala.util.Random

/**
  * Created by peng on 27/10/16.
  */
object APMSim {

  val existing: caching.ConcurrentSet[APMSim] = caching.ConcurrentSet()

  final val HOME = LocationGlobal(43.694195, -79.262262, 136)
  def scatteredHome = HOME.copy(
    lat = HOME.lat + (Random.nextDouble() - 0.5)*0.0002,
    lon = HOME.lon + (Random.nextDouble() - 0.5)*0.0002
  )

  /**
    * 43.694195,-79.262262,136,353
    */
  def defaultHomeStr = {
    val coordinate = scatteredHome.productIterator.toSeq
    val yaw = Random.nextDouble()*360.0
    (coordinate ++ Seq(yaw)).mkString(",")
  } //Somewhere between Toronto and Scarborough

  def next(home: String = defaultHomeStr): APMSim = this.synchronized {
    val nextINumOpt = (0 to 254).find{
      i =>
        !existing.map(_.iNum).toSeq.contains(i)
    }
    val nextINum = nextINumOpt
      .getOrElse(
        throw new UnsupportedOperationException("APMSim iNum depleted")
      )
    APMSim(nextINum, home)
  }
}

case class APMSim private (
                            iNum: Int,
                            home: String,
                            baudRate: Int = MAVConf.DEFAULT_BAUDRATE
                          ) extends CaseInstanceRef with SingletonRef with LocalCleanable {

  APMSim.existing += this

  override def cleanImpl() = {
    super.cleanImpl()
    APMSim.existing -= this
  }
}