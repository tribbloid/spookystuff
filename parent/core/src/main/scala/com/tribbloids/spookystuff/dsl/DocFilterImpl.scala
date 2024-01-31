package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.DocFilter
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Agent

//TODO: support chaining & extends ExpressionLike/TreeNode
sealed trait DocFilterImpl extends DocFilter {

  def assertStatusCode(page: Doc): Unit = {
    page.httpStatus.foreach { v =>
      assert(v.getStatusCode.toString.startsWith("2"), v.toString)
    }
  }
}

object DocFilterImpl {

  case object Bypass extends DocFilterImpl {

    override def apply(v: (Doc, Agent)): Doc = {
      v._1
    }
  }

  case object AcceptStatusCode2XX extends DocFilterImpl {

    override def apply(v: (Doc, Agent)): Doc = {
      val result = v._1
      assertStatusCode(result)
      result
    }
  }

  case object MustHaveTitle extends DocFilterImpl {

    override def apply(v: (Doc, Agent)): Doc = {
      val result = v._1
      assertStatusCode(result)
      if (result.mimeType.contains("html")) {
        assert(
          result.root.\("html").\("title").text.getOrElse("").nonEmpty,
          s"Html Page @ ${result.uri} has no title" +
            result.root.code
              .map { code =>
                ":\n" + code.slice(0, 500) + "\n..."
              }
              .getOrElse("")
        )
      }
      result
    }
  }
}
