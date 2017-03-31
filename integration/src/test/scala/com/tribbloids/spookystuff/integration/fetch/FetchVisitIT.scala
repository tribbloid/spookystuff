package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationFixture

class FetchVisitIT extends IntegrationFixture {

  override def doMain() {

    val RDD = spooky
      .fetch(
        Visit("http://www.wikipedia.org/")
      )
      .persist()

    val pageRows = RDD.unsquashedRDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(pageRows.length === 1)
    assert(pageRows(0).docs.length === 1)
    assert(pageRows(0).docs.head.uri contains "://www.wikipedia.org/")
    assert(pageRows(0).docs.head.name === Snapshot(DocFilters.MustHaveTitle).toString)
    val pageTime = pageRows(0).docs.head.timeMillis
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from s3 cache

    val RDD2 = RDD
      .fetch(
        Visit("http://www.wikipedia.org/") +> Snapshot() ~ 'b
      )

      .persist()

    val unionRDD = RDD.union(RDD2)
    val unionRows = unionRDD.unsquashedRDD.collect()

    assert(unionRows.length === 2)
    assert(unionRows(0).docs.head.copy(timeMillis = 0, raw = null, saved = null)
      === unionRows(1).docs.head.copy(timeMillis = 0, raw = null, saved = null))

    assert(unionRows(0).docs.head.timeMillis === unionRows(1).docs.head.timeMillis)
    assert(unionRows(0).docs.head.raw === unionRows(1).docs.head.raw)
    assert(unionRows(0).getOnlyDoc.get.raw === unionRows(1).docs.head.raw)
    assert(unionRows(0).getOnlyDoc.get.name === Snapshot(DocFilters.MustHaveTitle).toString)
    assert(unionRows(1).getOnlyDoc.get.name === "b")

    //this is to ensure that an invalid expression (with None interpolation result) won't cause loss of information
    val RDDfetchNone = unionRDD
      .fetch(
        Visit('noSuchField) +> Snapshot() ~ 'c
      )

    val fetchNoneRows = RDDfetchNone.unsquashedRDD.collect()

    assert(fetchNoneRows.length === 2)
    assert(fetchNoneRows(0).docs.length === 0)
    assert(fetchNoneRows(1).docs.length === 0)
  }

  override def numPages= spooky.conf.defaultGenPartitioner match {
//    case FetchOptimizers.WebCacheAware => 1
    case _ => 1
  }

  override def pageFetchedCap: Int = 3
}