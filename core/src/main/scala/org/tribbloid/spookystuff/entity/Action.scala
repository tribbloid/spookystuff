package org.tribbloid.spookystuff.entity

/**
 * Created by peng on 04/06/14.
 */
//click/input/select/wait/snapshot
//abstract class Action extends Serializable {
abstract class Action {
  var timer: Boolean = true

  def setTimer(on: Boolean = true) = { this.timer = on}
}

//represents an action that potentially changes a page
abstract class Interaction extends Action
case class Visit(val url: String) extends Interaction
case class Wait(val seconds: Int) extends Interaction
case class Click(val selector: String, val repeat: Int = 1) extends Interaction
case class Submit(val selector: String, val repeat: Int = 1) extends Interaction
case class Input(val selector: String, val content: String) extends Interaction
case class Select(val selector: String, val content: String) extends Interaction

case class Snapshot(val name: String = null) extends Action
case class Screenshot(val name: String = null) extends Action //screenshot feature is disabled