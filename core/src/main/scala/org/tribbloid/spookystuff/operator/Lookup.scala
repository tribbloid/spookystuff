package org.tribbloid.spookystuff.operator

import java.net.URLEncoder
import java.util.UUID

import org.tribbloid.spookystuff.Utils
import org.tribbloid.spookystuff.entity.PageUID
import org.tribbloid.spookystuff.entity.client.Action

/**
 * Created by peng on 9/12/14.
 */
abstract class Lookup[T] extends (PageUID => T) with Serializable with Product

case object VerbosePathLookup extends Lookup[String] {

  override def apply(uid: PageUID): String = {

    val actionStrs = uid.backtrace.map(_.toString)

    val actionConcat = if (actionStrs.size > 4) {
      val oneTwoThree = actionStrs.slice(0,3)
      val last = actionStrs.last
      val omitted = "..."+(uid.backtrace.length-4).toString+"more"+"..."

      oneTwoThree.mkString("~")+omitted+last
    }
    else actionStrs.mkString("~")

    val hash = "-"+uid.backtrace.hashCode

    Utils.canonizeFileName(actionConcat + hash)
  }
}

case object HierarchicalUrnLookup extends Lookup[String] {

  override def apply(uid: PageUID): String = {

    val actionStrs = uid.backtrace.map(_.toString)

    val actionConcat = if (actionStrs.size > 4) {
      val oneTwoThree = actionStrs.slice(0,3)
      val last = actionStrs.last
      val omitted = "/"+(uid.backtrace.length-4).toString+"more"+"/"

      oneTwoThree.mkString("/")+omitted+last
    }
    else actionStrs.mkString("/")

    val hash = "-"+uid.backtrace.hashCode

    Utils.canonizeUrn(actionConcat + hash)
  }
}