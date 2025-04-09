package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.{CommonUtils, Timeout}
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import com.tribbloids.spookystuff.{ActionException, Const}
import com.tribbloids.spookystuff.actions.Export.DocValidation.StatusCode2XX

import scala.concurrent.duration.Duration
import scala.concurrent.{duration, TimeoutException}

class ActionSuite extends SpookyBaseSpec {

  import ActionSuite.*

  import duration.*

  describe("human readable SerDe") {

    val action = Wget("http://dummy.com").accept(StatusCode2XX)

    it("JSON") {
      val str = action.prettyJSON()
      str.shouldBe(
        """
          |{
          |  "original" : {
          |    "uri" : "http://dummy.com"
          |  },
          |  "validation" : { }
          |}
        """.stripMargin
      )
    }

    it("treeText") {
      val str = action.treeText
      str.shouldBe(
        """
          |Accept
          |  Wget
          |    "http://dummy.com"
          |  StatusCode2XX
        """.stripMargin
      )
    }
  }

  it("Timed mixin can terminate execution if it takes too long") {

    val a = MockAlwaysTimeout
    val session = new Agent(this.spooky)
    assert(
      a.getTimeout(session).hardTerimination == spookyConf.remoteResourceTimeout.max + Timeout.hardTerminateOverhead
    )
    val b = a in 10.seconds
    assert(
      b.getTimeout(session).hardTerimination == 10.seconds + Timeout.hardTerminateOverhead
    )

    val (_, time) = CommonUtils.timed {
      try {
        b.fetch(spooky)
        sys.error("impossible")
      } catch {
        case e: ActionException =>
          println(e)
          assert(e.getCause.isInstanceOf[TimeoutException])
      }
    }
    assert(time <= 40 * 1000 * Const.remoteResourceLocalRetries)
  }
}

object ActionSuite {

  case object MockAlwaysTimeout extends Export with MayTimeout {

    override def doExe(agent: Agent): Seq[Observation] = {
      Thread.sleep(120 * 1000)
      Nil
    }
  }

  case class MockInteraction(info: String) extends Interaction with MayTimeout {

    override def cooldown: Duration = Duration.Zero

    override def exeNoOutput(agent: Agent): Unit = {}
  }

  case class MockExport() extends Export {

    override def doExe(agent: Agent): Seq[Observation] = {
      Seq(
        Doc(
          DocUID(agent.backtrace.toSeq)(),
          "http://dummy.com",
          Some("text/html; charset=UTF-8")
        )().setRaw("<html></html>".getBytes("UTF8"))
      )
    }
  }
}
