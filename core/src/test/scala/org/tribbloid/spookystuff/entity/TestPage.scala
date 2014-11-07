package org.tribbloid.spookystuff.entity

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions.{Snapshot, Visit, Wget}
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.factory.driver.NaiveDriverFactory

import scala.concurrent.duration._

/**
 * Created by peng on 10/17/14.
 */
class TestPage extends FunSuite with BeforeAndAfter {

  val prop = new Properties()
  prop.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
  val AWSAccessKeyId = prop.getProperty("AWSAccessKeyId")
  val AWSSecretKey = prop.getProperty("AWSSecretKey")

  val conf: SparkConf = new SparkConf().setAppName("test")
    .setMaster("local[*]")

  val sc: SparkContext = {
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration
      .set("fs.s3n.awsAccessKeyId", AWSAccessKeyId)
    sc.hadoopConfiguration
      .set("fs.s3n.awsSecretAccessKey", AWSSecretKey)

    sc
  }

  val spooky = {

    val sql: SQLContext = new SQLContext(sc)
    val spooky: SpookyContext = new SpookyContext(
      sql,
      driverFactory = NaiveDriverFactory(loadImages = true)
    )
    spooky.autoSave = false
    spooky.autoCache = false
    spooky.autoRestore = false

    spooky
  }

  override def finalize(){
    sc.stop()
  }

  val page = {
    val builder = new PageBuilder(spooky)
    val res = try {
      builder += Visit("http://en.wikipedia.org")
      builder += Snapshot()
      builder.pages
    }
    finally{
      builder.close()
    }

    res
  }

  val wgetPage = {
    val builder = new PageBuilder(spooky)

    val res = try {
      builder += Wget("http://en.wikipedia.org")
      builder.pages
    }
    finally {
      builder.close()
    }

    res
  }

  test ("local cache") {
    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
    spooky.pageExpireAfter = 2.seconds

    Page.autoCache(page, page.head.uid.backtrace, spooky)

    val page2 = Page.autoRestoreLatest(page.head.uid.backtrace, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)

    Thread.sleep(2000)

    val page3 = Page.autoRestoreLatest(page.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)
  }

  test ("wget local cache") {
    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
    spooky.pageExpireAfter = 2.seconds

    Page.autoCache(wgetPage, wgetPage.head.uid.backtrace, spooky)

    val page2 = Page.autoRestoreLatest(wgetPage.head.uid.backtrace, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === wgetPage.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)

    Thread.sleep(2000)

    val page3 = Page.autoRestoreLatest(wgetPage.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === wgetPage.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)
  }

  test ("s3 cache") {

    spooky.setRoot(s"s3n://spooky-unit/")
    spooky.pageExpireAfter = 10.seconds

    Page.autoCache(page, page.head.uid.backtrace, spooky)

    val page2 = Page.autoRestoreLatest(page.head.uid.backtrace, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)

    Thread.sleep(12000)

    val page3 = Page.autoRestoreLatest(page.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.contentStr === page2.head.contentStr)
  }
}
