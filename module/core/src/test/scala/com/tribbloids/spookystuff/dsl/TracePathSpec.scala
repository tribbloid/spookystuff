package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.ActionSuite.{MockExport, MockInteraction}
import com.tribbloids.spookystuff.io.CrossPlatformFileUtils
import com.tribbloids.spookystuff.testutils.{FileURIDocsFixture, SpookyBaseSpec}

class TracePathSpec extends SpookyBaseSpec with FileURIDocsFixture {

  import scala.concurrent.duration.*

  {
    // FileURIDocsFixture is mixed in as a trait, so HTML_URL is available directly
    val htmlUrl: String = HTML_URL

    // TODO: add more non-primary-construtor params
    // TODO: move to core module
    val doc1 = spooky
      .fetch(_ =>
        MockInteraction(htmlUrl) +>
          MockInteraction("input#searchInput") +>
          MockExport().as("A")
      )
      .rdd
      .map(_.trajectory.docs)
      .first()
      .head

    val doc2 = spooky
      .fetch(_ =>
        MockInteraction(htmlUrl) +>
          MockInteraction("input#searchInput").in(40.seconds) +>
          MockExport().as("A")
      )
      .rdd
      .map(_.trajectory.docs)
      .first()
      .head

    val byTraces = Seq(
      TracePath.Flat,
      TracePath.Hierarchical
    )
    val byDocs = Seq(
      DocPath.TimeStampName(TracePath.Hierarchical),
      DocPath.UUIDName(TracePath.Hierarchical)
    )

    // TODO: merge repetition
    byTraces.foreach { byTrace =>
      val encoded1 = byTrace.apply(doc1.uid.backtrace)
      val encoded2 = byTrace.apply(doc2.uid.backtrace)

      it(s"${byTrace.getClass.getSimpleName} should not encode action parameters that are not in primary constructor") {
        assert(encoded1 == encoded2)
      }

      it(s"${byTrace.getClass.getSimpleName} should not yield string containing new line character") {
        assert(!encoded1.contains('\n'))
      }

      it(s"${byTrace.getClass.getSimpleName} should not use default Function.toString") {
        assert(!encoded1.contains("Function"))
      }
    }

    byDocs.foreach { byDoc =>
      val encoded1 = byDoc.apply(doc1)
      val encoded2 = byDoc.apply(doc2)

      it(s"${byDoc.getClass.getSimpleName} should not encode action parameters that are not in primary constructor") {
        // Use cross-platform path splitting for consistency
        val separator = CrossPlatformFileUtils.fileSeparator
        val path1Components = CrossPlatformFileUtils.splitPath(encoded1).toSeq.slice(0, -1)
        val path2Components = CrossPlatformFileUtils.splitPath(encoded2).toSeq.slice(0, -1)
        assert(
          path1Components == path2Components,
          s"Path components differ:\nPath1: $path1Components\nPath2: $path2Components\nOriginal1: $encoded1\nOriginal2: $encoded2"
        )
      }

      it(s"${byDoc.getClass.getSimpleName} should not yield string containing new line character") {
        assert(!encoded1.contains('\n'))
      }

      it(s"${byDoc.getClass.getSimpleName} should not use default Function.toString") {
        assert(!encoded1.contains("Function"))
      }
    }
  }

}
