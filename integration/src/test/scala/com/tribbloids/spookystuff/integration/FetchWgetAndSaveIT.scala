package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.pages.PageUtils

/**
 * Created by peng on 11/26/14.
 */
class FetchWgetAndSaveIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext) {

    val RDD = spooky
      .fetch(
        Wget("http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/220px-Wikipedia-logo-v2.svg.png")
      )
      .select("Wikipedia" ~ 'name)
      .persist()
      .savePages(x"file://${System.getProperty("user.home")}/spooky-integration/save/${'name}", overwrite = true)
      .select(S.saved ~ 'saved_path)
      .persist()

    val savedPageRows = RDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(savedPageRows.length === 1)
    assert(savedPageRows(0).pages.length === 1)
    val pageTime = savedPageRows(0).pages.head.timestamp.getTime
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from s3 cache

    val content = savedPageRows(0).pages.head.content

    assert(savedPageRows(0).get("saved_path").get.asInstanceOf[Iterable[Any]].head === s"file:${System.getProperty("user.home")}/spooky-integration/save/Wikipedia.png")

    val loadedContent = PageUtils.load(s"file://${System.getProperty("user.home")}/spooky-integration/save/Wikipedia.png")(spooky)

    assert(loadedContent === content)

    Thread.sleep(10000) //this delay is necessary to circumvent eventual consistency of HDFS-based cache

    val RDDAppended = RDD
      .fetch(
        Wget("http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/220px-Wikipedia-logo-v2.svg.png").as('b),
        joinType = Append
      )

    val appendedRows = RDDAppended.collect()

    assert(appendedRows.length === 2)
    assert(appendedRows(0).pages.apply(0).copy(timestamp = null, content = null, saved = null) === appendedRows(1).pages.apply(0).copy(timestamp = null, content = null, saved = null))

    assert(appendedRows(0).pages(0).timestamp === appendedRows(1).pages(0).timestamp)
    assert(appendedRows(0).pages(0).content === appendedRows(1).pages.apply(0).content)
    assert(appendedRows(0).pages.apply(0).content === appendedRows(1).pages.apply(0).content)
    assert(appendedRows(1).pages.apply(0).name === "b")
  }

  override def numPages = {
    case Wide_RDDWebCache => 1
    case _ => 2
  }

  override def numDrivers = 0
}