package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.{Interaction, PythonAction}
import com.tribbloids.spookystuff.session.Session

import scala.concurrent.duration.Duration

/**
  * Created by peng on 26/08/16.
  */
case class Move(
                 from: GlobalLocation,
                 to: GlobalLocation,
                 override val delay: Duration = null
               ) extends PythonAction with Interaction {

  override def exeNoOutput(session: Session): Unit = {
    this.Py(session).exe()
  }
}