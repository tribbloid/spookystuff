package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.ITBaseSpec
import com.tribbloids.spookystuff.web.actions.{DropDownSelect, Snapshot, Submit, TextInput, Visit}
import org.scalatest.Ignore

import java.net.URLEncoder

/**
  * Created by peng on 12/14/14.
  */
@Ignore // waiting for scalaJS rewrite
class FetchInteractionsIT extends ITBaseSpec {

  override def doMain(): Unit = {

    val chain = (
      Visit("http://www.wikipedia.org")
        +> TextInput("input#searchInput", "深度学习")
        +> DropDownSelect("select#searchLanguage", "zh")
        +> Submit("button.pure-button")
    )

    val RDD = spooky
      .fetch(
        chain
      )
      .persist()

    val pageRows = RDD.fetchedRDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(pageRows.length === 1)
    assert(pageRows(0).docs.size === 1)
    val uri = pageRows(0).docs.head.uri
    assert(
      (uri endsWith "zh.wikipedia.org/wiki/深度学习") || (uri endsWith "zh.wikipedia.org/wiki/" + URLEncoder
        .encode("深度学习", "UTF-8"))
    )
    assert(pageRows(0).docs.head.name === Snapshot(DocFilterImpl.MustHaveTitle).toString)
    val pageTime = pageRows(0).observations.head.timeMillis
    assert(pageTime < finishTime)
    assert(pageTime > finishTime - 120000) // long enough even after the second time it is retrieved from s3 cache

    Thread.sleep(10000) // this delay is necessary to circumvent eventual consistency of HDFS-based cache

    val RDD2 = RDD
      .fetch(
        chain
          +> Snapshot() ~ 'b
      )

    val unionRDD = RDD.union(RDD2)
    val unionRows = unionRDD.fetchedRDD.collect()

    assert(unionRows.length === 2)
    assert(
      unionRows(0).docs.head.copy(timeMillis = 0)(content = null)
        === unionRows(1).docs.head.copy(timeMillis = 0)(content = null)
    )

    unionRows.map(_.docs.head.timeMillis.toString).shouldBeIdentical()
    unionRows.map(_.docs.head.content.contentStr).shouldBeIdentical()

    assert(unionRows(0).docs.head.name === Snapshot(DocFilterImpl.MustHaveTitle).toString)
    assert(unionRows(1).docs.head.name === "b")
  }

  override def numPages: Long = spooky.conf.localityPartitioner match {
//    case WebCacheAware => 1
    case _ => 1
  }
}
