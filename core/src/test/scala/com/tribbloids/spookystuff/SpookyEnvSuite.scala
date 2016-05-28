package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.{DriverFactories, DriverFactory}
import com.tribbloids.spookystuff.execution.SchemaContext
import com.tribbloids.spookystuff.extractors.{Alias, GenExtractor, GenResolved}
import com.tribbloids.spookystuff.row.{SquashedFetchedRow, TypedField}
import com.tribbloids.spookystuff.tests.{RemoteDocsMixin, TestHelper}
import com.tribbloids.spookystuff.utils.Utils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Retries}

import scala.language.implicitConversions

abstract class SpookyEnvSuite
  extends FunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Retries
    with RemoteDocsMixin {

  def sc: SparkContext = TestHelper.TestSpark
  def sql: SQLContext = TestHelper.TestSQL
  lazy val spookyConf = new SpookyConf(
    driverFactory = driverFactory
  )
  lazy val spooky = new SpookyContext(sql, spookyConf)
  lazy val schema = SchemaContext(spooky)

  implicit def wSpooky(row: SquashedFetchedRow): SquashedFetchedRow#W = new row.W(schema)
  implicit def extractor2Resolved[T, R](extractor: Alias[T, R]): GenResolved[T, R] = GenResolved(
    extractor.resolve(schema),
    TypedField(
      extractor.field,
      extractor.applyType(schema)
    )
  )

  implicit def extractor2Function[T, R](extractor: GenExtractor[T, R]): PartialFunction[T, R] = extractor.resolve(schema)

  lazy val driverFactory: DriverFactory = DriverFactories.PhantomJS(loadImages = true)

  override def withFixture(test: NoArgTest) = {
    if (isRetryable(test))
      Utils.retry(4) { super.withFixture(test) }
    else
      super.withFixture(test)
  }

  override def beforeAll() {

    val conf = TestHelper.TestSparkConf.setAppName("test")

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }

    TestHelper.clearTempDir()
    super.afterAll()
  }

  before{
    setUp()
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