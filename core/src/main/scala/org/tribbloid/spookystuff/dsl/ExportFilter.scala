package org.tribbloid.spookystuff.dsl

import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.session.Session

/**
 * Created by peng on 26/07/15.
 */
trait ExportFilter extends ((Page, Session) => Page) with Serializable {

  override def toString() = this.getClass.getSimpleName.replace("$","")
}

object ExportFilters {

  case object PassAll extends ExportFilter {

    override def apply(result: Page, session: Session): Page = {
      result
    }
  }
  
  case object MustHaveTitle extends ExportFilter {

    override def apply(result: Page, session: Session): Page = {
      if (result.mimeType.contains("html")){
        assert(result.\("html").\("title").text.getOrElse("").nonEmpty, s"Html Page @ ${result.uri} has no title:\n${result.code}")
      }
      result
    }
  }
}