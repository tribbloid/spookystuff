package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.{Lineage, FetchedRow, Field}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import com.tribbloids.spookystuff.commons.{CommonUtils, Timeout}
import com.tribbloids.spookystuff.{ActionException, Const}

import scala.collection.immutable.ListMap
import scala.concurrent.{duration, TimeoutException}
import scala.util.Random

class ActionSuite extends SpookyBaseSpec {

  import duration._
  import ActionSuite._

  it("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Timeout(Random.nextInt().seconds)
    val action = Wget(x"${'A}").in(randomTimeout)

    val rewritten = action
      .interpolate(FetchedRow(Lineage(data = ListMap(Field("A") -> "http://www.dummy.com")), Seq()), emptySchema)
      .get

    assert(rewritten === Wget(Lit.erased("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

  it("interpolate should not change name") {

    val action = Wget(x"${'A}").as('dummy_name)

    val rewritten = action
      .interpolate(FetchedRow(Lineage(data = ListMap(Field("A") -> "http://www.dummy.com")), Seq()), emptySchema)
      .get

    assert(rewritten === Wget(Lit.erased("http://www.dummy.com")))
    assert(rewritten.name === "dummy_name")
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

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
