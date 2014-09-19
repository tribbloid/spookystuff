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

    val actions = uid.backtrace.slice(0,3).mkString("~")

    val truncated = if (actions.length<=200) actions
    else actions.substring(0,200)

    val suffix = if (uid.backtrace.length <=3 ) ""
    else "..."+(uid.backtrace.length-3).toString+"more"

    val hash = "-"+uid.backtrace.hashCode

    val block = if (uid.blockKey == -1) ""
    else "/"+uid.blockKey

    Utils.canonize(truncated + suffix + hash) + block
  }
}