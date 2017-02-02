package com.tribbloids.spookystuff.uav.sim

import com.tribbloids.spookystuff.caching
import com.tribbloids.spookystuff.uav.actions.LocationGlobal
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
    lat = HOME.lat + (Random.nextDouble() - 0.5)*0.001,
    lon = HOME.lon + (Random.nextDouble() - 0.5)*0.001
  )

  final val FRAMERATE = 200
  final val SPEEDUP = 5

  /**
    * 43.694195,-79.262262,136,353
    */
  def defaultHomeStr = {
    val coordinate = scatteredHome.productIterator.toSeq
    val yaw = Random.nextDouble()*360.0
    (coordinate ++ Seq(yaw)).mkString(",")
  } //Somewhere between Toronto and Scarborough

  def next(
            extraArgs: Seq[String] = Seq(
              "--model", "quad",
              "--gimbal",
              "--home", APMSim.defaultHomeStr
            ),
            vType: String = "copter",
            version: String = "3.3"
          ): APMSim = this.synchronized {
    val nextINumOpt = (0 to 254).find{
      i =>
        !existing.map(_.iNum).toSeq.contains(i)
    }
    val nextINum = nextINumOpt
      .getOrElse(
        throw new UnsupportedOperationException("APMSim iNum depleted")
      )
    APMSim(nextINum, extraArgs = extraArgs, vType = vType, version = version)
  }
}

case class APMSim private (
                            iNum: Int,
                            extraArgs: Seq[String],
                            rate: Int = APMSim.FRAMERATE,
                            speedup: Int = APMSim.SPEEDUP,
                            vType: String = "copter",
                            version: String = "3.3"
                          ) extends CaseInstanceRef with SingletonRef with LocalCleanable {

  APMSim.existing += this

  override def cleanImpl() = {
    super.cleanImpl()
    APMSim.existing -= this
  }
}