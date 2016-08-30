package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.DriverFactory
import com.tribbloids.spookystuff.extractors.{Alias, GenExtractor, GenResolved}
import com.tribbloids.spookystuff.row.{DataRowSchema, SquashedFetchedRow, TypedField}
import com.tribbloids.spookystuff.testutils.{RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.openqa.selenium.WebDriver
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Retries}

import scala.language.implicitConversions

abstract class SpookyEnvFixture
  extends FunSuite
      with BeforeAndAfter
      with BeforeAndAfterAll
      with Retries
      with RemoteDocsFixture {

    def sc: SparkContext = TestHelper.TestSpark
    def sql: SQLContext = TestHelper.TestSQL
    lazy val spookyConf = new SpookyConf(
      webDriverFactory = driverFactory
    )
    lazy val spooky = new SpookyContext(sql, spookyConf)
    lazy val schema = DataRowSchema(spooky)

    implicit def withSchema(row: SquashedFetchedRow): SquashedFetchedRow#WithSchema = new row.WithSchema(schema)
    implicit def extractor2Resolved[T, R](extractor: Alias[T, R]): GenResolved[T, R] = GenResolved(
      extractor.resolve(schema),
      TypedField(
        extractor.field,
        extractor.resolveType(schema)
      )
    )

    implicit def extractor2Function[T, R](extractor: GenExtractor[T, R]): PartialFunction[T, R] = extractor.resolve(schema)

    lazy val driverFactory: DriverFactory[WebDriver] = SpookyConf.DEFAULT_WEBDRIVER_FACTORY

    override def withFixture(test: NoArgTest) = {
      if (isRetryable(test))
        SpookyUtils.retry(4) { super.withFixture(test) }
      else
        super.withFixture(test)
    }

    override def beforeAll() {

      TestHelper.TestSparkConf.setAppName("test")

      super.beforeAll()
    }

    override def afterAll() {
  //    if (sc != null) {
  //      sc.stop()
  //    }//TODO: remove it: sc implementation no longer recreates

      TestHelper.clearTempDir()
      super.afterAll()
    }

    before{
      // bypass java.lang.NullPointerException at org.apache.spark.broadcast.TorrentBroadcast$.unpersist(TorrentBroadcast.scala:228)
      // TODO: clean up after fix
      SpookyUtils.retry(50) {
        setUp()
      }
    }

    def setUp(): Unit = {
      spooky.conf = new SpookyConf(
        autoSave = true,
        cacheWrite = false,
        cacheRead = false,
        dirs = new DirConf(
          root = TestHelper.tempPath + "spooky-unit/"
        )
      )
    }
  }