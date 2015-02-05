package org.tribbloid.spookystuff.dsl

import java.util.UUID

import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.utils.Utils

/**
 * Created by peng on 8/29/14.
 */

//only from Page
sealed abstract class Extract[+R] extends (Page => R) with Serializable

class UUIDFileName(encoder: TraceEncoder[_]) extends Extract[String] {
  override def apply(page: Page): String =
    Utils.uriConcat(encoder(page.uid.backtrace).toString, UUID.randomUUID().toString)
}