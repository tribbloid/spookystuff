package org.tribbloid.spookystuff.entity

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.entity.client.{Wget, Snapshot, Visit}
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.factory.driver.NaiveDriverFactory

import scala.concurrent.duration._

/**
 * Created by peng on 10/17/14.
 */
class TestPage extends FunSuite with BeforeAndAfter {

  val spooky = {
    val conf: SparkConf = new SparkConf().setAppName("test")
      .setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)
    val sql: SQLContext = new SQLContext(sc)
    val spooky: SpookyContext = new SpookyContext(
      sql,
      driverFactory = NaiveDriverFactory(loadImages = true)
    )

    spooky
  }

  val page = {
    val builder = new PageBuilder(spooky)()
    builder += Visit("http://en.wikipedia.org")

    val res = Snapshot().doExe(builder)

    builder.finalize()
    res
  }

  val wgetPage = {
    val builder = new PageBuilder(spooky)()
    val res = Wget("http://en.wikipedia.org").doExe(builder)

    builder.finalize()
    res
  }

  test ("local cache") {
    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
    spooky.pageExpireAfter = 2.seconds

    Page.autoCache(page, page.head.uid, spooky)

    val page2 = Page.autoRestoreLatest(page.head.uid, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)

    Thread.sleep(2000)

    val page3 = Page.autoRestoreLatest(page.head.uid, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)
  }

  test ("wget local cache") {
    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
    spooky.pageExpireAfter = 2.seconds

    Page.autoCache(wgetPage, wgetPage.head.uid, spooky)

    val page2 = Page.autoRestoreLatest(wgetPage.head.uid, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === wgetPage.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)

    Thread.sleep(2000)

    val page3 = Page.autoRestoreLatest(wgetPage.head.uid, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === wgetPage.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)
  }

  test ("s3 cache") {
    spooky.setRoot("s3n://a:a@spooky-unit/")
    spooky.pageExpireAfter = 10.seconds

    Page.autoCache(page, page.head.uid, spooky)

    val page2 = Page.autoRestoreLatest(page.head.uid, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)

    Thread.sleep(12000)

    val page3 = Page.autoRestoreLatest(page.head.uid, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)
  }
}
