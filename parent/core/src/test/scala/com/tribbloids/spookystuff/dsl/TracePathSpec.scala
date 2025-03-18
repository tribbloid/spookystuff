package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.ActionSuite.{MockExport, MockInteraction}
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, RemoteDocsFixture, SpookyBaseSpec}

import java.io.File

class TracePathSpec extends SpookyBaseSpec {

  import scala.concurrent.duration.*

  val resources: FileDocsFixture.type = FileDocsFixture

  {
    val r = resources
    import r.*

    // TODO: add more non-primary-construtor params
    // TODO: move to core module
    val doc1 = spooky
      .fetch(_ =>
        MockInteraction(HTML_URL) +>
          MockInteraction("input#searchInput") +>
          MockExport().as("A")
      )
      .rdd
      .map(_.docs)
      .first()
      .head

    val doc2 = spooky
      .fetch(_ =>
        MockInteraction(HTML_URL) +>
          MockInteraction("input#searchInput").in(40.seconds) +>
          MockExport().as("A")
      )
      .rdd
      .map(_.docs)
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
        assert(encoded1.split(File.separator).toSeq.slice(0, -1) == encoded2.split(File.separator).toSeq.slice(0, -1))
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
