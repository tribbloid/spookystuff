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
    this.+(as: _*)
  }

  // everything in this list is formatted
  val actions: util.List[Action] = new util.ArrayList()

  def +(as: Action*): ActionPlan = {
    as.foreach{
      a => this.actions.add(a.format(context))
    }
    return this
  }

  //will remove context of the parameter! cannot merge two context as they may have conflict keys
  def +(ac: ActionPlan): ActionPlan = {
    return this.+(ac.actions: _*)
  }

  def +[T <: Serializable](key: String, value: T){
    this.context.put(key,value)
  }

  def interactions = actions.collect{
    case i: Interaction => i
  }.toSeq

  //only execute interactions and extract the final stage
  def !(): Page = {
    val page = PageBuilder.resolveFinal(this.interactions: _*).modify(context = this.context)
    return page
  }

  //execute
  def !!!(): Seq[Page] = {
    var pages = PageBuilder.resolve(this.actions: _*)
    if (this.context !=null) {
      //has to use deep copy, one to many mapping and context may be modified later
      pages = pages.map { _.modify(context = new util.HashMap(this.context)) }
    }
    return pages
  }
}
