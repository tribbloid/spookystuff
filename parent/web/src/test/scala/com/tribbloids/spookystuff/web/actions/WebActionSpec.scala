package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.{ActionException, QueryException}
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.actions.ControlBlock.LocalRetry
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.web.conf.Web
import org.apache.spark.rdd.RDD

import java.util.Date
import scala.concurrent.duration

class WebActionSpec extends SpookyBaseSpec with FileDocsFixture {

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

  describe("WebDriver errorDump") {

    it("won't be triggered if no webDriver was initialized") {
      try {
        DefectiveWebExport.fetch(spooky)
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
          assert(e.getMessage.contains("Snapshot/Bypass()/DefectiveWebExport"))
          assert(e.getMessage.contains("Screenshot/Bypass()/DefectiveWebExport"))
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
          assert(e.getMessage.contains("Delay/1_second/Snapshot/Bypass()/DefectiveWebExport"))
          assert(e.getMessage.contains("Delay/1_second/Screenshot/Bypass()/DefectiveWebExport"))
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

      intercept[QueryException] {
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

      intercept[QueryException] {
        (Delay(10.seconds)
          +> Visit(HTML_URL)
          +> Screenshot().waybackToTimeMillis(dates.head - 2000)).fetch(spooky)
      }
    }
  }
}

object WebActionSpec {

  case object DefectiveWebExport extends Export with WebAction {

    override def doExeNoName(agent: Agent): Seq[Observation] = {
      agent.driverOf(Web)
      sys.error("error")
    }
  }
}
