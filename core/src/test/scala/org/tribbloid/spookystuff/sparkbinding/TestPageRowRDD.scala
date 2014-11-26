package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions.Wget
import org.tribbloid.spookystuff.factory.driver.NaiveDriverFactory
import org.tribbloid.spookystuff.expressions._

/**
 * Created by peng on 11/26/14.
 */
class TestPageRowRDD extends FunSuite {

  val conf: SparkConf = new SparkConf().setAppName("test")
    .setMaster("local[*]")

  val sc: SparkContext = new SparkContext(conf)

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

  test("inherited RDD operation") {
    import spooky._

    val texts = noInput
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .select('* text "div.central-featured-lang" as '~)
      .flatMap(_.get("~").get.asInstanceOf[Seq[String]])
      .collect()

    assert(texts.size === 10)
    assert(texts(0).startsWith("English"))
  }
}