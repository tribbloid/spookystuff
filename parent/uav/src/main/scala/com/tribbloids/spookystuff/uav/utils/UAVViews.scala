package com.tribbloids.spookystuff.uav.utils

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.telemetry.{Dispatcher, Link}

import scala.util.Try

/**
  * Created by peng on 06/05/17.
  */
object UAVViews {

  /**
    * if left Nil will randomly choose any one from the fleet. can be changed by Locality to enforce globally
    * optimal execution. if task already has a drone (TaskLocal) and its not in this list, will throw an error!
    * Locality can detect this early
    */
  implicit class SessionView(session: Session) {

    val uavConf: UAVConf = session.spooky.getConf[UAVConf]

    val linkTry: Try[Link] = {
      Dispatcher(
        uavConf.uavsInFleetShuffled,
        session,
        Binding.Transient(None)
      ).tryGet
    }

    val link: Link = {
      linkTry.get
    }
  }
}
