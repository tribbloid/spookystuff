package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.DocumentFilter
import com.tribbloids.spookystuff.pages.Page
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.PrettyToStringMixin
import org.slf4j.LoggerFactory

//TODO: support chaining & extends ExpressionLike/TreeNode
trait AbstractDocumentFilter extends DocumentFilter with PrettyToStringMixin {

  def assertStatusCode(page: Page){
    page.httpStatus.foreach {
      v =>
        assert(v.getStatusCode.toString.startsWith("2"), v.toString)
    }
  }
}

object DocumentFilters {

  case object Status2XX extends AbstractDocumentFilter {

    override def apply(result: Page, session: Session): Page = {
      assertStatusCode(result)
      result
    }
  }

  case object MustHaveTitle extends AbstractDocumentFilter {

    override def apply(result: Page, session: Session): Page = {
      assertStatusCode(result)
      if (result.mimeType.contains("html")){
        assert(result.\("html").\("title").text.getOrElse("").nonEmpty, s"Html Page @ ${result.uri} has no title")
        LoggerFactory.getLogger(this.getClass).info(s"Html Page @ ${result.uri} has no title:\n${result.code}")
      }
      result
    }
  }
}