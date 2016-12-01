package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.DriverFactory
import com.tribbloids.spookystuff.extractors.{Alias, GenExtractor, GenResolved}
import com.tribbloids.spookystuff.row.{DataRowSchema, SquashedFetchedRow, TypedField}
import com.tribbloids.spookystuff.session.{Cleanable, CleanWebDriver, Lifespan}
import com.tribbloids.spookystuff.testutils.{RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.jutils.jprocesses.JProcesses
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Retries}

import scala.language.implicitConversions

object SpookyEnvFixture {
  //  def cleanDriverInstances(): Unit = {
  //    CleanMixin.unclean.foreach {
  //      tuple =>
  //        tuple._2.foreach (_.finalize())
  //        assert(tuple._2.isEmpty)
  //    }
  //  }

  def shouldBeClean(
                     spooky: SpookyContext,
                     pNames: Seq[String]
                   ): Unit = {

    driverInstancesShouldBeClean(spooky)
    driverProcessShouldBeClean(pNames)
  }

  def driverInstancesShouldBeClean(spooky: SpookyContext): Unit = {

    Cleanable
      .uncleaned
      .foreach {
        tuple =>
          val nonLocalDrivers = tuple._2
            .filter {
              v =>
                v.lifespan.isInstanceOf[Lifespan.Task]
            }
          assert(
            nonLocalDrivers.isEmpty,
            s": ${tuple._1} is unclean! ${nonLocalDrivers.size} left:\n" + nonLocalDrivers.mkString("\n")
          )
      }
  }

  /**
    * slow
    */
  def driverProcessShouldBeClean(
                                  pNames: Seq[String]
                                ): Unit = {

    //this is necessary as each suite won't automatically cleanup drivers NOT in task when finished
    Cleanable.cleanSweepAll (
      condition = {
        case v if v.lifespan.isThread => true
        case _ => false
      }
    )

    import scala.collection.JavaConverters._

    val processes = JProcesses.getProcessList()
      .asScala

    pNames.foreach {
      name =>
        val matchedProcess = processes.filter(_.getName == name)
        assert(
          matchedProcess.isEmpty,
          s"${matchedProcess.size} $name process(es) left:\n" + matchedProcess.mkString("\n")
        )
    }
  }
}

abstract class SpookyEnvFixture
  extends RemoteDocsFixture
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Retries {

  //  val startTime = System.currentTimeMillis()

  def sc: SparkContext = TestHelper.TestSpark
  def sql: SQLContext = TestHelper.TestSQL

  @transient lazy val spookyConf = new SpookyConf(
    webDriverFactory = driverFactory
  )

  var _spooky: SpookyContext = _
  def spooky = {
    Option(_spooky)
      //      .filterNot(_.sparkContext.isStopped) TODO: not compatible with 1.5
      .getOrElse {
      val result = new SpookyContext(sql, spookyConf)
      _spooky = result
      result
    }
  }

  def schema = DataRowSchema(spooky)

  implicit def withSchema(row: SquashedFetchedRow): SquashedFetchedRow#WithSchema = new row.WithSchema(schema)
  implicit def extractor2Resolved[T, R](extractor: Alias[T, R]): GenResolved[T, R] = GenResolved(
    extractor.resolve(schema),
    TypedField(
      extractor.field,
      extractor.resolveType(schema)
    )
  )

  implicit def extractor2Function[T, R](extractor: GenExtractor[T, R]): PartialFunction[T, R] = extractor.resolve(schema)

  lazy val driverFactory: DriverFactory[CleanWebDriver] = SpookyConf.TEST_WEBDRIVER_FACTORY

  override def withFixture(test: NoArgTest) = {
    if (isRetryable(test))
      SpookyUtils.retry(4) { super.withFixture(test) }
    else
      super.withFixture(test)
  }

  import com.tribbloids.spookystuff.utils.SpookyViews.SparkContextView

  override def beforeAll() {

    TestHelper.TestSparkConf.setAppName("Test:" + this.getClass.getSimpleName)

    super.beforeAll()
  }

  val pNames = Seq("phantomjs", "python")

  override def afterAll() {

    val spooky = this.spooky
    val pNames = this.pNames
    TestHelper.clearTempDir()
    sc.foreachComputer {
      SpookyEnvFixture.shouldBeClean(spooky, pNames)
    }
    super.afterAll()
  }

  before{
    // bypass java.lang.NullPointerException at org.apache.spark.broadcast.TorrentBroadcast$.unpersist(TorrentBroadcast.scala:228)
    // TODO: clean up after fix
    SpookyUtils.retry(10) {
      setUp()
    }
  }

  after{
    tearDown()
  }

  def setUp(): Unit = {
    val spooky = this.spooky
    //    SpookyEnvFixture.cleanDriverInstances()
    spooky.conf = new SpookyConf(
      autoSave = true,
      cacheWrite = false,
      cacheRead = false,
      components = envComponents
    )
    spooky.metrics.zero()
  }

  def envComponents: Components[AbstractConf] = {
    Components(
      new DirConf(
        root = SpookyUtils.\\\(TestHelper.TEMP_PATH, "spooky-unit")
      )
    )
  }

  def tearDown(): Unit = {
    val spooky = this.spooky
    sc.foreachComputer {
      SpookyEnvFixture.driverInstancesShouldBeClean(spooky)
    }
  }
}