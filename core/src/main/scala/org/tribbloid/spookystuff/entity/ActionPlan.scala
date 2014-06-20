package org.tribbloid.spookystuff.entity

import java.util
import java.io.Serializable
import scala.collection.JavaConversions._

/**
* Created by peng on 12/06/14.
*/
class ActionPlan(val context: util.Map[String, Serializable] = null) extends Serializable {

  def this(context: util.Map[String, Serializable], as: Action*) = {
    this(context)
    this.+=(as: _*)
  }

  // everything in this list is formatted
  val actions: util.List[Action] = new util.ArrayList()

  override def equals(a: Any): Boolean = a match {
    case a: ActionPlan => {
      if ((this.context == a.context) && (this.actions == a.actions)) return true
      else return false
    }
    case _ => return false
  }

  override def toString(): String = {
    "ActionPlan("+this.context.toString+","+this.actions.toString+")"
  }

  def +=(as: Action*) {
    as.foreach{
      a => this.actions.add(a.format(context))
    }
  }

  //will remove context of the parameter! cannot merge two context as they may have conflict keys
  def +=(ac: ActionPlan) {
    this.+=(ac.actions: _*)
  }

  def + (as: Action*): ActionPlan = {
    val result = new ActionPlan(this.context, this.actions: _*)
    result.+=(as: _*)
    result
  }

  def + (ac: ActionPlan): ActionPlan = {
    val result = new ActionPlan(this.context, this.actions: _*)
    result.+=(ac.actions: _*)
    result
  }

  //  def +[T <: Serializable](key: String, value: T){
  //    this.context.put(key,value)
  //  }

  def interactions = actions.collect{
    case i: Interactive => i
  }.toSeq

  //only execute interactions and extract the final stage, if has no action will return an empty page
  def !(): Page = {
    val page = PageBuilder.resolveFinal(this.interactions: _*).modify(context = this.context)
    return page
  }

  //execute, left: if nothing extracted will return an empty page, not left: will return nothing
  def !!!(left: Boolean = false): Seq[Page] = {
    var pages = PageBuilder.resolve(this.actions: _*)
    if (pages.size==0 && left ==true) pages = pages.:+(PageBuilder.emptyPage())
    if (this.context !=null) {
      //has to use deep copy, one to many mapping and context may be modified later
      pages = pages.map { _.modify(context = new util.HashMap(this.context)) }
    }
    return pages
  }
}
