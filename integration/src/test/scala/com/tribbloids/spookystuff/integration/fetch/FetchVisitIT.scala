package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationSuite

class FetchVisitIT extends IntegrationSuite {

  import com.tribbloids.spookystuff.utils.ImplicitUtils._

  override def doMain() {

    val RDD = spooky
      .fetch(
        Visit("http://www.wikipedia.org/")
      )
      .persist()

    val pageRows = RDD.unsquashedRDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(pageRows.length === 1)
    assert(pageRows(0).pages.length === 1)
    assert(pageRows(0).pages.head.uri contains "://www.wikipedia.org/")
    assert(pageRows(0).pages.head.name === Snapshot(DocFilters.MustHaveTitle).toString)
    val pageTime = pageRows(0).pages.head.timeMillis.getTime
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
    assert(unionRows(0).pages.head.copy(timeMillis = null, content = null, saved = null)
      === unionRows(1).pages.head.copy(timeMillis = null, content = null, saved = null))

    assert(unionRows(0).pages.head.timeMillis === unionRows(1).pages.head.timeMillis)
    assert(unionRows(0).pages.head.content === unionRows(1).pages.head.content)
    assert(unionRows(0).getOnlyPage.get.content === unionRows(1).pages.head.content)
    assert(unionRows(0).getOnlyPage.get.name === Snapshot(DocFilters.MustHaveTitle).toString)
    assert(unionRows(1).getOnlyPage.get.name === "b")

    //this is to ensure that an invalid expression (with None interpolation result) won't cause loss of information
    val RDDfetchNone = unionRDD
      .fetch(
        Visit('noSuchField) +> Snapshot() ~ 'c
      )

    val fetchNoneRows = RDDfetchNone.unsquashedRDD.collect()

    assert(fetchNoneRows.length === 2)
    assert(fetchNoneRows(0).pages.length === 0)
    assert(fetchNoneRows(1).pages.length === 0)
  }

  override def numPages= spooky.conf.defaultFetchOptimizer match {
//    case FetchOptimizers.WebCacheAware => 1
    case _ => 1
  }
}