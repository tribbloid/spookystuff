package com.tribbloids.spookystuff.mav


import com.tribbloids.spookystuff.actions.Interaction
import com.tribbloids.spookystuff.session.Session

import scala.concurrent.duration.Duration

/**
  * must have exactly the same class/function under the same package imported in python that takes 2 JSON strings
  * 1 for action, 1 for session
  */
abstract class PythonInteraction extends Interaction {

  override def exeNoOutput(session: Session): Unit = {

    val python = session.pythonDriver
    val result = python.interpret(
      s"""
         |${this.getClass.getCanonicalName}('${this.toJSON}')
       """.trim.stripMargin
    )
  }
}

/**
  * Created by peng on 26/08/16.
  */
case class Move(
                 from: GlobalLocation,
                 to: GlobalLocation,
                 override val delay: Duration = null
               ) extends PythonInteraction {


}