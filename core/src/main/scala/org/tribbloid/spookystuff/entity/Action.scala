package org.tribbloid.spookystuff.entity

import org.tribbloid.spookystuff.conf.Conf

/**
 * Created by peng on 04/06/14.
 */
//click/input/select/wait/snapshot
//abstract class Action extends Serializable {
abstract class Action {
  val timer: Boolean = true

//  def setTimer(on: Boolean = true) = { this.timer = on}
}

//TODO: Seriously, I don't know how to use these fancy things with case class & pattern matching
//trait Delayed {
//  val delay: Int = Conf.pageDelay
//}

//trait Repeated {
//  var repeat: Int = 1
//}

//represents an action that potentially changes a page
abstract class Interaction extends Action

case class Visit(val url: String) extends Interaction
//Better safely delay than sorry
case class Delay(val delay: Int = Conf.pageDelay) extends Interaction
case class DelayFor(val selector: String,val delay: Int) extends Interaction

case class Click(val selector: String) extends Interaction
case class Submit(val selector: String) extends Interaction
case class Input(val selector: String, val content: String) extends Interaction
case class Select(val selector: String, val content: String) extends Interaction

case class Snapshot(val name: String = null) extends Action
case class Screenshot(val name: String = null) extends Action //screenshot feature is disabled