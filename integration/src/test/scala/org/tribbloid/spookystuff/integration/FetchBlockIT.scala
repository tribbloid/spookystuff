package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 11/26/14.
 */
class FetchBlockIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    DriverFactories.PhantomJS() //TODO: HtmlUnit does not support Backbone.js
  )

  override def doMain(spooky: SpookyContext) {

    val RDD = spooky
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
          +> Snapshot().as('a)
          +> Loop (
          ClickNext("button.btn","1"::Nil)
            +> Snapshot().as('b)
        ),
        flattenPagesPattern = null
      ).persist()

    val pageRows = RDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(pageRows.length === 1)
    assert(pageRows(0).pages.length === 3)
    assert(pageRows(0).pages(0).name === "a")
    assert(pageRows(0).pages(1).name === "b")
    assert(pageRows(0).pages(2).name === "b")
    val pageTime = pageRows(0).pages.head.timestamp.getTime
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from s3 cache

    Thread.sleep(10000) //this delay is necessary to circumvent eventual consistency of HDFS-based cache

    val RDDAppended = RDD
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
          +> Snapshot().as('c)
          +> Loop (
          ClickNext("button.btn","1"::Nil)
            +> Snapshot().as('d)
        ),
        joinType = Append,
        flattenPagesPattern = null
      )

    val appendedRows = RDDAppended.collect()

    assert(appendedRows.length === 1)
    assert(appendedRows(0).pages.length === 6)
    assert(appendedRows(0).pages(3).name === "c")
    assert(appendedRows(0).pages(4).name === "d")
    assert(appendedRows(0).pages(5).name === "d")
  }

  override def numPages = {
    case Wide_RDDWebCache => 3
    case _ => 6
  }

  override def numSessions = 1
}