package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.pages.Page
import com.tribbloids.spookystuff.session.Session
import org.slf4j.LoggerFactory

/**
 * Created by peng on 26/07/15.
 */
trait DocumentFilter extends ((Page, Session) => Page) with Serializable {

  override def toString() = this.getClass.getSimpleName.replace("$","")
}

object DocumentFilter {

  case object PassAll extends DocumentFilter {

    override def apply(result: Page, session: Session): Page = {
      result
    }
  }

  case object MustHaveTitle extends DocumentFilter {

    override def apply(result: Page, session: Session): Page = {
      if (result.mimeType.contains("html")){
        assert(result.\("html").\("title").text.getOrElse("").nonEmpty, s"Html Page @ ${result.uri} has no title")
        LoggerFactory.getLogger(this.getClass).info(s"Html Page @ ${result.uri} has no title:\n${result.code}")
      }
      result
    }
  }
}