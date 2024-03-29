package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.ITBaseSpec
import com.tribbloids.spookystuff.web.actions.{Snapshot, Visit}

class FetchVisitIT extends ITBaseSpec {

  override def doMain(): Unit = {

    val RDD = spooky
      .fetch(
        Visit(HTML_URL)
      )
      .persist()

    val pageRows = RDD.fetchedRDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(pageRows.length === 1)
    assert(pageRows(0).docs.length === 1)
    assert(pageRows(0).docs.head.uri contains HTML_URL)
    assert(pageRows(0).docs.head.name === Snapshot(DocFilterImpl.MustHaveTitle).toString)
    val pageTime = pageRows(0).docs.head.timeMillis
    assert(pageTime < finishTime)
    assert(pageTime > finishTime - 60000) // long enough even after the second time it is retrieved from s3 cache

    val RDD2 = RDD
      .fetch(
        Visit(HTML_URL) +> Snapshot() ~ 'b
      )
      .persist()

    val unionRDD = RDD.union(RDD2)
    val unionRows = unionRDD.fetchedRDD.collect()

    assert(unionRows.length === 2)
    assert(
      unionRows(0).docs.head.copy(timeMillis = 0)(content = null)
        === unionRows(1).docs.head.copy(timeMillis = 0)(content = null)
    )

    unionRows.map(_.docs.head.timeMillis.toString).shouldBeIdentical()
    unionRows.map(_.docs.head.content.contentStr).shouldBeIdentical()
    unionRows.map(_.onlyDoc.get.content.contentStr).shouldBeIdentical()

    assert(unionRows(0).onlyDoc.get.name === Snapshot(DocFilterImpl.MustHaveTitle).toString)
    assert(unionRows(1).onlyDoc.get.name === "b")

    // this is to ensure that an invalid expression (with None interpolation result) won't cause loss of information
    val RDDfetchNone = unionRDD
      .fetch(
        Visit('noSuchField) +> Snapshot() ~ 'c
      )

    val fetchNoneRows = RDDfetchNone.fetchedRDD.collect()

    assert(fetchNoneRows.length === 2)
    assert(fetchNoneRows(0).docs.length === 0)
    assert(fetchNoneRows(1).docs.length === 0)
  }

  override def numPages: Long = spooky.conf.localityPartitioner match {
//    case FetchOptimizers.WebCacheAware => 1
    case _ => 1
  }

  override def pageFetchedCap: Long = 3
}
