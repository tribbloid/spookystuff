package com.tribbloids.spookystuff.web.actions

import ai.acyclic.prover.commons.debug.print_@
import com.tribbloids.spookystuff.ActionException
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.actions.ControlBlock.LocalRetry
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.testutils.{FileURIDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.web.conf.{Web, WebDriverFactory}
import org.apache.spark.rdd.RDD
import org.scalatest.Suite

import java.util.Date
import scala.concurrent.duration

class WebActionSpec extends SpookyBaseSpec with FileURIDocsFixture {

  import WebActionSpec.*

  import duration.*

  val exampleActionList: List[Action] = List(
    Click("dummy"),
    Wget("'{~}").as("dummy_name"),
    LocalRetry(
      Delay(10.seconds) +> Wget("ftp://www.dummy.org")
    )
  )

  // TODO: finish assertion
  exampleActionList.foreach { a =>
    it(s"${a.getClass.getSimpleName} has an UDT") {
      val rdd: RDD[(Selector, Action)] = sc.parallelize(Seq(("1": Selector) -> a))
      val df = sql.createDataFrame(rdd)

      df.show(false)
      df.printSchema()

      //        df.toJSON.collect().foreach(println)
    }
  }

  describe("errorDump") {

    it("won't be triggered if no webDriver was initialized") {
      try {
        DefectiveExport.fetch(spooky)
        sys.error("impossible")
      } catch {
        case e: ActionException =>
          assert(!e.getMessage.contains("Snapshot"))
          assert(!e.getMessage.contains("Screenshot"))
      }
    }

    it("will be triggered if webDriver is initialized, it should not be blocked by DocFilter") {
      try {
        DefectiveWebExport.fetch(spooky)
        sys.error("impossible")
      } catch {
        case e: ActionException =>
          println(e)
          assert(e.getMessage.contains("Snapshot/DefectiveWebExport"))
          assert(e.getMessage.contains("Screenshot/DefectiveWebExport"))
      }
    }

    it("should contains all backtraces") {
      try {
        (
          Delay(1.seconds) +>
            DefectiveWebExport
        ).fetch(spooky)
        sys.error("impossible")
      } catch {
        case e: ActionException =>
          println(e)
          assert(e.getMessage.contains("Delay/1_second/Snapshot/DefectiveWebExport"))
          assert(e.getMessage.contains("Delay/1_second/Screenshot/DefectiveWebExport"))
      }
    }
  }

  describe("Wayback should use old cache") {

    it("Snapshot") {

      spooky.confUpdate(
        _.copy(
          cacheWrite = true,
          IgnoreCachedDocsBefore = Some(new Date())
        )
      )

      val dates: Seq[Long] = (0 to 2).map { _ =>
        val pages = (
          Delay(10.seconds)
            +> Visit(HTML_URL)
            +> Snapshot()
        ).fetch(spooky) // 5s is long enough
        assert(pages.size == 1)
        pages.head.timeMillis
      }

      spooky.confUpdate(_.copy(cacheRead = true))

      val cachedPages = (Delay(10.seconds)
        +> Visit(HTML_URL)
        +> Snapshot().waybackToTimeMillis(dates(1) + 2000)).fetch(spooky)
      assert(cachedPages.size == 1)
      assert(cachedPages.head.timeMillis == dates(1))

      spooky.confUpdate(_.copy(remote = false))

      intercept[IllegalArgumentException] {
        (Delay(10.seconds)
          +> Visit(HTML_URL)
          +> Snapshot().waybackToTimeMillis(dates.head - 2000)).fetch(spooky)
      }
    }

    it("Screenshot") {

      spooky.confUpdate(
        _.copy(
          cacheWrite = true,
          IgnoreCachedDocsBefore = Some(new Date())
        )
      )

      val dates: Seq[Long] = (0 to 2).map { _ =>
        val pages = (Delay(10.seconds)
          +> Visit(HTML_URL)
          +> Screenshot()).fetch(spooky) // 5s is long enough
        assert(pages.size == 1)
        pages.head.timeMillis
      }

      spooky.confUpdate(_.copy(cacheRead = true))

      val cachedPages = (Delay(10.seconds)
        +> Visit(HTML_URL)
        +> Screenshot().waybackToTimeMillis(dates(1) + 2000)).fetch(spooky)
      assert(cachedPages.size == 1)
      assert(cachedPages.head.timeMillis == dates(1))

      spooky.confUpdate(_.copy(remote = false))

      intercept[IllegalArgumentException] {
        (Delay(10.seconds)
          +> Visit(HTML_URL)
          +> Screenshot().waybackToTimeMillis(dates.head - 2000)).fetch(spooky)
      }
    }
  }

  override lazy val nestedSuites: IndexedSeq[Suite] = {

    print_@(Chrome.TaskLocal)

    IndexedSeq(
      Chrome,
      Chrome.TaskLocal,
      FireFox,
      FireFox.TaskLocal
    )
  }
}

object WebActionSpec {

  object Chrome extends WebActionCase {

    override lazy val driverFactory = WebDriverFactory.Chrome()
  }

  object FireFox extends WebActionCase {

    override lazy val driverFactory = WebDriverFactory.Firefox()
  }

  case object DefectiveExport extends Export {

    override def doExe(agent: Agent): Seq[Observation] = {
      sys.error("error")
    }
  }

  case object DefectiveWebExport extends Export with WebAction {

    override def doExe(agent: Agent): Seq[Observation] = {
      agent.getDriver(Web)
      sys.error("error")
    }
  }
}
