package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.mav.telemetry.Link
import com.tribbloids.spookystuff.session.Session

trait MAVAction extends Action

/**
  * can only be constructed after Python Driver is initialized, otherwise throw NoPythonDriverException
  */
class MAVEXE(session: Session) {

  val mavConf: MAVConf = {
    session.spooky.conf.submodules.get[MAVConf]()
  }

  val link = {
    Link.getOrCreate(
      mavConf.endpoints,
      mavConf.proxyFactory,
      session
    )
  }

  val pyLink = link.Py(session)

}

class MAVInteractionEXE(session: Session) extends MAVEXE(session) {

  /**
    * when enclosed in an export, may behave differently.
    */
  def inbound(): Unit = {}

  def conduct(): Unit = {}

  def outbound(): Unit = {}
}

/**
  * inbound -> engage -> outbound
  *
  */
trait MAVInteraction extends Interaction with MAVAction {

  override def exeNoOutput(session: Session): Unit = {

    val impl = getImpl(session)
    impl.inbound()
    impl.conduct()
    impl.outbound()
  }

  def getImpl(session: Session): MAVInteractionEXE
}