package org.tribbloid.spookystuff.integration

import org.apache.hadoop.fs.Path
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.pages.PageUtils

import scala.concurrent.duration

/**
 * Created by peng on 11/26/14.
 */
class FetchWgetAndSaveIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext) {

    import spooky.dsl._

    val RDD = spooky
      .fetch(
        Wget("http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/220px-Wikipedia-logo-v2.svg.png")
      )
      .select("Wikipedia.png" ~ 'name)
      .savePages(x"file://${System.getProperty("user.home")}/spooky-integration/save/${'name}", overwrite = true)
      .select($.saved ~ 'saved_path)
      .persist()

    val savedPageRows = RDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(savedPageRows.size === 1)
    assert(savedPageRows(0).pages.size === 1)
    val pageTime = savedPageRows(0).pages.head.timestamp.getTime
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from s3 cache

    val content = savedPageRows(0).pages.head.content

    assert(savedPageRows(0).get("saved_path").get.asInstanceOf[Iterable[Any]].head === s"file:${System.getProperty("user.home")}/spooky-integration/save/Wikipedia.png")

    val loadedContent = PageUtils.load(new Path(s"file://${System.getProperty("user.home")}/spooky-integration/save/Wikipedia.png"))(spooky)

    assert(loadedContent === content)

    val RDDAppended = RDD
      .fetch(
        Wget("http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/220px-Wikipedia-logo-v2.svg.png").as('b),
        joinType = Append
      )

    val appendedRows = RDDAppended.collect()

    assert(appendedRows.size === 2)
    assert(appendedRows(0).pages.apply(0).copy(timestamp = null, content = null, saved = null) === appendedRows(1).pages.apply(0).copy(timestamp = null, content = null, saved = null))

    import scala.concurrent.duration._
    if (spooky.conf.defaultQueryOptimizer != Narrow && spooky.conf.pageExpireAfter >= 10.minutes) {
      assert(appendedRows(0).pages(0).timestamp === appendedRows(1).pages(0).timestamp)
      assert(appendedRows(0).pages(0).content === appendedRows(1).pages.apply(0).content)
    }
    assert(appendedRows(0).pages.apply(0).content === appendedRows(1).pages.apply(0).content)
    assert(appendedRows(1).pages.apply(0).name === "b")
  }

  override def numPages = {
    case Narrow => 2
    case _ => 1
  }

  override def numDrivers = _ => 0
}