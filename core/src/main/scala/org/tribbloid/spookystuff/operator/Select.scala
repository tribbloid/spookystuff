package org.tribbloid.spookystuff.operator

import java.net.URLEncoder

import org.tribbloid.spookystuff.entity.Page

/**
* Created by peng on 8/29/14.
*/
abstract class Select[T] extends (Page => T)

object SelectUrlEncodingPath extends Select[String] {

  override def apply(page: Page): String = {
    val suffix = if (page.backtrace == null||page.backtrace.size<=2) ""
    else "-"+page.backtrace.hashCode().toString

    var uid = URLEncoder.encode((page.resolvedUrl + suffix).replaceAll("[:\\\\/]+", "*"), "UTF-8")
    if (uid.length>255) uid = uid.substring(0,255)

    var timeid = page.timestamp.getTime.toString
    if (timeid.length>255) timeid = timeid.substring(0,255)

    uid+"/"+timeid
  }
}