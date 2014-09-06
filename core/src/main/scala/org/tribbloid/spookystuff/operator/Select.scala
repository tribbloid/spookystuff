package org.tribbloid.spookystuff.operator

import org.tribbloid.spookystuff.entity.Page

/**
* Created by peng on 8/29/14.
*/
abstract class Select[T] extends (Page => T)

//object UrlEncodingPagePath extends Select[String] {
//
//  override def apply(page: Page): String = {
//    val suffix = if (page.backtrace.size<=2) ""
//    else page.backtrace.hashCode().toString
//
//    val body = page.resolvedUrl
//  }
//}