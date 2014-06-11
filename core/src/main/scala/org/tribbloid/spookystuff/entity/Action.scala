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
//TODO: considering nested structure for maximum control
abstract class Interaction extends Action

case class Visit(val url: String) extends Interaction
//Better safely delay than sorry
case class Delay(val delay: Int = Conf.pageDelay) extends Interaction
case class DelayFor(val selector: String,val delay: Int) extends Interaction

case class Click(val selector: String) extends Interaction
case class Submit(val selector: String) extends Interaction
case class Input(val selector: String, val content: String) extends Interaction
case class Select(val selector: String, val content: String) extends Interaction

abstract class Extraction extends Action
case class Snapshot() extends Extraction
//only useful for extracting temporary things, not recommened for organized extraction, TODO: not tested!

abstract class Dump extends Action
case class Insert(val key: String, val value: String) extends Dump
case class Wget(val url: String, val name: String, val path: String = Conf.savePagePath) extends Dump
//case class Screenshot(val name: String = null) extends Extraction //screenshot feature is disabled
case class GetText(val selector: String) extends Dump
case class GetLink(val selector: String) extends Dump
case class GetSrc(val selector: String) extends Dump
case class GetAttr(val selector: String, val attr: String) extends Dump