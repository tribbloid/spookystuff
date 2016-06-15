package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}

class FilePathsSuite extends SpookyEnvSuite{

  import dsl._
  import scala.concurrent.duration._

  //TODO: add more non-primary-construtor params
  val doc1 = spooky
    .fetch(
      Visit(HTML_URL) +>
        WaitFor("input#searchInput") +>
        Snapshot().as('A)
    )
    .unsquashedRDD
    .map(_.pages)
    .first().head

  val doc2 = spooky
    .fetch(
      Visit(HTML_URL) +>
        WaitFor("input#searchInput").in(40.seconds) +>
        Snapshot().as('A)
    )
    .unsquashedRDD
    .map(_.pages)
    .first().head

  val byTraces = Seq(
    FilePaths.Flat,
    FilePaths.Hierarchical
  )

  byTraces.foreach {
    byTrace =>
      val encoded1 = byTrace.apply(doc1.uid.backtrace)
      val encoded2 = byTrace.apply(doc2.uid.backtrace)

      test(s"${byTrace.getClass.getSimpleName} should not encode action parameters that are not in primary constructor") {
        println(encoded1)
        assert(encoded1 == encoded2)
      }

      test(s"${byTrace.getClass.getSimpleName} should not yield string containing new line character") {
        assert(!encoded1.contains('\n'))
      }
  }

  val byDocs = Seq(
    FilePaths.TimeStampName(FilePaths.Hierarchical),
    FilePaths.UUIDName(FilePaths.Hierarchical)
  )

  byDocs.foreach {
    byDoc =>

      val encoded1 = byDoc.apply(doc1)
      val encoded2 = byDoc.apply(doc2)

      test(s"${byDoc.getClass.getSimpleName} should not yield string containing new line character") {
        assert(!encoded1.contains('\n'))
      }
  }
}
