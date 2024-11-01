package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.{CommonUtils, Timeout}
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.row.FetchedRow
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import com.tribbloids.spookystuff.{ActionException, Const}

import scala.collection.immutable.ListMap
import scala.concurrent.{duration, TimeoutException}
import scala.util.Random

class ActionSuite extends SpookyBaseSpec {

  import ActionSuite._

  import duration._

  describe("Wget") {
    val action = Wget("http://dummy.com")

    it("-> JSON") {
      val str = action.prettyJSON()
      str.shouldBe(
        """
          |{
          |  "uri" : "http://dummy.com",
          |  "filter" : { }
          |}
        """.stripMargin
      )
    }

    it("-> treeText") {
      val str = action.treeText
      str.shouldBe(
        """
          |Wget
          |  "http://dummy.com"
          |  MustHaveTitle()
        """.stripMargin
      )
    }
  }

  it("Timed mixin can terminate execution if it takes too long") {

    val a = OverdueExport
    val session = new Agent(this.spooky)
    assert(
      a.hardTerminateTimeout(session).max == spookyConf.remoteResourceTimeout.max + Const.hardTerminateOverhead
    )
    a in 10.seconds
    assert(
      a.hardTerminateTimeout(session).max == 10.seconds + Const.hardTerminateOverhead
    )

    val (_, time) = CommonUtils.timed {
      try {
        a.fetch(spooky)
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

  case object OverdueExport extends Export with Timed.ThreadSafe {

    override def doExeNoName(agent: Agent): Seq[Observation] = {
      Thread.sleep(120 * 1000)
      Nil
    }
  }

}
