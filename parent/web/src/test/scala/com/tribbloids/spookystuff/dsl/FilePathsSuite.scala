package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import java.io.File
import com.tribbloids.spookystuff.web.actions.{Snapshot, Visit, WaitFor}

class FilePathsSuite extends SpookyBaseSpec {

  import scala.concurrent.duration.*

  // TODO: add more non-primary-construtor params
  val doc1 = spooky
    .fetch(
      Visit(HTML_URL) +>
        WaitFor("input#searchInput") +>
        Snapshot().as('A)
    )
    .fetchedRDD
    .map(_.docs)
    .first()
    .head

  val doc2 = spooky
    .fetch(
      Visit(HTML_URL) +>
        WaitFor("input#searchInput").in(40.seconds) +>
        Snapshot().as('A)
    )
    .fetchedRDD
    .map(_.docs)
    .first()
    .head

  val byTraces = Seq(
    FilePaths.Flat,
    FilePaths.Hierarchical
  )
  val byDocs = Seq(
    FilePaths.TimeStampName(FilePaths.Hierarchical),
    FilePaths.UUIDName(FilePaths.Hierarchical)
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
