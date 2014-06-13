package org.tribbloid.spookystuff.entity

import java.util
import scala.collection.JavaConversions._

/**
 * Created by peng on 12/06/14.
 */
class ActionChain(val context: util.Map[String,String] = new util.HashMap()) extends Serializable {

  // everything in this list is formatted
  val actions: util.List[Action] = new util.ArrayList()

  def +(as: Action*): ActionChain = {
    as.foreach{
      a => this.actions.add(a.format(context))
    }
    return this
  }

  //will remove context of the parameter! cannot merge two context as they may have conflict keys
  def +(ac: ActionChain): ActionChain = {
    return this.+(ac.actions: _*)
  }

  def +(key: String, value: String){
    this.context.put(key,value)
  }

  //delegate to Action
//  private def resolve(a: Action, key: String, value: String){
//
//  }
}
