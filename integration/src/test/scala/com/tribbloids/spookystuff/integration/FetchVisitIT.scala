package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._

/**
 * Created by peng on 11/26/14.
 */
class FetchVisitIT extends IntegrationSuite {

  override def doMain(spooky: SpookyContext) {

    val RDD = spooky
      .fetch(
        Visit("http://www.wikipedia.org/")
      ).persist()

    val pageRows = RDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(pageRows.length === 1)
    assert(pageRows(0).pages.length === 1)
    assert(pageRows(0).pages.apply(0).uri contains "://www.wikipedia.org/")
    assert(pageRows(0).pages.apply(0).name === "Snapshot(MustHaveTitle,null)")
    val pageTime = pageRows(0).pages.head.timestamp.getTime
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from s3 cache

    val RDDAppended = RDD
      .fetch(
        Visit("http://www.wikipedia.org/") +> Snapshot() ~ 'b,
        joinType = Append
      ).persist()

    val appendedRows = RDDAppended.collect()

    assert(appendedRows.length === 2)
    assert(appendedRows(0).pages(0).copy(timestamp = null, content = null, saved = null)
      === appendedRows(1).pages(0).copy(timestamp = null, content = null, saved = null))

    assert(appendedRows(0).pages(0).timestamp === appendedRows(1).pages(0).timestamp)
    assert(appendedRows(0).pages(0).content === appendedRows(1).pages.apply(0).content)
    assert(appendedRows(0).getOnlyPage.get.content === appendedRows(1).pages.apply(0).content)
    assert(appendedRows(0).getOnlyPage.get.name === "Snapshot(MustHaveTitle,null)")
    assert(appendedRows(1).getOnlyPage.get.name === "b")

    //this is to ensure that an invalid expression (with None interpolation result) won't cause loss of information
    val RDDfetchNone = RDDAppended
      .fetch(
        Visit('noSuchField) +> Snapshot() ~ 'c
      )

    val fetchNoneRows = RDDfetchNone.collect()

    assert(fetchNoneRows.length === 2)
    assert(fetchNoneRows(0).pages.length === 0)
    assert(fetchNoneRows(1).pages.length === 0)
  }

  override def numFetchedPages = {
    case Wide_RDDWebCache => 1
    case _ => 2
  }
}