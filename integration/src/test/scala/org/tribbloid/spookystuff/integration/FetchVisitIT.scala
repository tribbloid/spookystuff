package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

import scala.concurrent.duration

/**
 * Created by peng on 11/26/14.
 */
class FetchVisitIT extends IntegrationSuite {

  override def doMain(spooky: SpookyContext) {

    import spooky._

    val RDD = spooky
      .fetch(
        Visit("http://www.wikipedia.org/")
      ).persist()

    val pageRows = RDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(pageRows.size === 1)
    assert(pageRows(0).pages.size === 1)
    assert(pageRows(0).pages.apply(0).uri === "http://www.wikipedia.org/")
    assert(pageRows(0).pages.apply(0).name === "Snapshot()")
    val pageTime = pageRows(0).pages.head.timestamp.getTime
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from s3 cache

    val RDDAppended = RDD
      .fetch(
        Visit($.uri) +> Snapshot() ~ 'b,
        joinType = Append
      ).persist()

    val appendedRows = RDDAppended.collect()

    assert(appendedRows.size === 2)
    assert(appendedRows(0).pages(0).copy(timestamp = null, content = null, saved = null)
      === appendedRows(1).pages(0).copy(timestamp = null, content = null, saved = null))

    import duration._
    if (spooky.conf.defaultQueryOptimizer != Narrow && spooky.conf.pageExpireAfter >= 10.minutes) {
      assert(appendedRows(0).pages(0).timestamp === appendedRows(1).pages(0).timestamp)
      assert(appendedRows(0).pages(0).content === appendedRows(1).pages.apply(0).content)
    }
    assert(appendedRows(0).getOnlyPage.get.content === appendedRows(1).pages.apply(0).content)
    assert(appendedRows(0).getOnlyPage.get.name === "Snapshot()")
    assert(appendedRows(1).getOnlyPage.get.name === "b")

    //this is to ensure that an invalid expression (with None interpolation result) won't cause loss of information
    val RDDfetchNone = RDDAppended
      .fetch(
        Visit('noSuchField) +> Snapshot() ~ 'c
      )

    val fetchNoneRows = RDDfetchNone.collect()

    assert(fetchNoneRows.size === 2)
    assert(fetchNoneRows(0).pages.size === 0)
    assert(fetchNoneRows(1).pages.size === 0)
  }

  override def numPages = {
    case Narrow => 2
    case _ => 1
  }
}