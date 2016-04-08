package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationSuite

/**
  * Created by peng on 11/26/14.
  */
class FetchPaginationIT extends IntegrationSuite {

  import com.tribbloids.spookystuff.utils.Implicits._

  override lazy val drivers = Seq(
    phantomJS //TODO: HtmlUnit does not support Backbone.js
  )

  override def doMain() {

    val RDD = spooky
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
          +> Snapshot().as('a)
          +> Loop (
          ClickNext("button.btn","1"::Nil)
            +> Snapshot().as('b)
        )
      )
      .persist()

    val pageRows = RDD.unsquashedRDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(pageRows.length === 2) //TODO: adapt to new default grouping: ab b
    assert(pageRows(0).pages.map(_.name) === Seq("a", "b"))
    assert(pageRows(1).pages.map(_.name) === Seq("b"))
    val pageTime = pageRows(0).pages.head.timestamp.getTime
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from DFS cache

    Thread.sleep(10000) //this delay is necessary to circumvent eventual consistency of DFS cache

    val RDD2 = RDD
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
          +> Snapshot().as('c)
          +> Loop (
          ClickNext("button.btn","1"::Nil)
            +> Snapshot().as('d)
        )
      )

    val pageRows2 = RDD2.unsquashedRDD.collect()

    assert(pageRows2.length === 2)
    assert(pageRows2(0).pages.map(_.name) === Seq("c", "d"))
    assert(pageRows2(1).pages.map(_.name) === Seq("d"))
  }

  override def numPages= spooky.conf.defaultFetchOptimizer match {
//    case FetchOptimizers.WebCacheAware => 3
    case _ => 3
  }

  override def numSessions = 1
}