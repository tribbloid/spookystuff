package org.tribbloid.spookystuff.entity

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions.Snapshot
import org.tribbloid.spookystuff.factory.PageBuilder

/**
 * Created by peng on 22/06/14.
 */

class TestEmptyPage extends FunSuite with BeforeAndAfter {

  val conf: SparkConf = new SparkConf().setAppName("test")
    .setMaster("local[*]")

  val sc: SparkContext = {
    new SparkContext(conf)
  }

  val sql: SQLContext = new SQLContext(sc)

  //shorthand for resolving the final stage after some interactions
  lazy val emptyPage: Page = {
    val pb = new PageBuilder(new SpookyContext(sql: SQLContext))

    try {
      pb.getDriver
      Snapshot().doExe(pb).toList(0)
    }
    finally {
      pb.close()
    }
  }

  var page: Page = null

  before {
    page = emptyPage
  }

  test("attr1") {assert (page.attr1("div.dummy","href") === null)}

  test("attr") {assert (page.attr("div.dummy","href") === Seq[String]())}

  test("text1") {assert (page.text1("div.dummy") === null)}

  test("text") {assert (page.text("div.dummy") === Seq[String]())}

  test("slice") {assert (page.slice("div.dummy")(10) === Seq[Page]())}

  test("elementExist") {assert (page.elementExist("div.dummy") === false)}

  test("attrExist") {assert (page.attrExist("div.dummy","href") === false)}
}