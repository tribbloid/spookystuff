package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
* Created by peng on 11/26/14.
*/
class FetchVisitIT extends IntegrationSuite {

  override def doMain(spooky: SpookyContext) {

    import spooky._

    val RDD = noInput
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
        Visit("http://www.wikipedia.org/") +> Snapshot() ~ 'b,
        joinType = Append
      )

    val appendedRows = RDDAppended.collect()

    assert(appendedRows.size === 2)
    assert(appendedRows(0).pages.apply(0).copy(content = null) === appendedRows(1).pages.apply(0).copy(content = null))
    assert(appendedRows(0).pages.apply(0).content === appendedRows(1).pages.apply(0).content)
    assert(appendedRows(0).pages.apply(0).name === "Snapshot()")
    assert(appendedRows(1).pages.apply(0).name === "b")
  }

  override def numPages: Int = 1
}