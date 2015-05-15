package org.tribbloid.spookystuff.dsl

import java.util.UUID

import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.utils.Utils

/**
 * Created by peng on 5/14/15.
 */
object PageFilePaths {

  //only from Page
  case class UUIDName(encoder: CacheFilePath[Any]) extends PageFilePath[String] {
    override def apply(page: Page): String =
      Utils.uriConcat(encoder(page.uid.backtrace).toString, UUID.randomUUID().toString)
  }

  case class TimeStampName(encoder: CacheFilePath[Any]) extends PageFilePath[String] {
    override def apply(page: Page): String =
      Utils.uriConcat(encoder(page.uid.backtrace).toString, Utils.canonizeFileName(page.timestamp.toString))
  }
}
