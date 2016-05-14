package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationSuite
import com.tribbloids.spookystuff.doc.PageUtils

class FetchWgetAndSaveIT extends IntegrationSuite {

  import com.tribbloids.spookystuff.utils.Implicits._

  override lazy val drivers = Seq(
    null
  )

  override def doMain() {

    val RDD = spooky
      .fetch(
        Wget("http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/220px-Wikipedia-logo-v2.svg.png")
      )
      .select("Wikipedia" ~ 'name)
      .persist()
      .savePages(x"file://${System.getProperty("user.dir")}/temp/spooky-integration/save/${'name}", overwrite = true)
      .select(S.saved ~ 'saved_path)
      .persist()

    val savedPageRows = RDD.unsquashedRDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(savedPageRows.length === 1)
    assert(savedPageRows(0).pages.length === 1)
    val pageTime = savedPageRows(0).pages.head.timestamp.getTime
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from s3 cache

    val content = savedPageRows(0).pages.head.content

    assert(
      savedPageRows(0).dataRow.get('saved_path).get.asInstanceOf[Iterable[Any]].toSeq contains
        s"file:${System.getProperty("user.dir")}/temp/spooky-integration/save/Wikipedia.png"
    )

    val loadedContent = PageUtils.load(s"file://${System.getProperty("user.dir")}/temp/spooky-integration/save/Wikipedia.png")(spooky)

    assert(loadedContent === content)

    Thread.sleep(10000) //this delay is necessary to circumvent eventual consistency of HDFS-based cache

    val RDD2 = RDD
      .fetch(
        Wget("http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/220px-Wikipedia-logo-v2.svg.png").as('b)
      )

    val unionRDD = RDD.union(RDD2)
    val unionRows = unionRDD.unsquashedRDD.collect()

    assert(unionRows.length === 2)
    assert(unionRows(0).pages.head.copy(timestamp = null, content = null, saved = null) === unionRows(1).pages.head.copy(timestamp = null, content = null, saved = null))

    assert(unionRows(0).pages.head.timestamp === unionRows(1).pages.head.timestamp)
    assert(unionRows(0).pages.head.content === unionRows(1).pages.head.content)
    assert(unionRows(0).pages.head.content === unionRows(1).pages.head.content)
    assert(unionRows(1).pages.head.name === "b")
  }

  override def numPages= spooky.conf.defaultFetchOptimizer match {
//    case FetchOptimizers.WebCacheAware => 1
    case _ => 1
  }

  override def numDrivers = 0
}