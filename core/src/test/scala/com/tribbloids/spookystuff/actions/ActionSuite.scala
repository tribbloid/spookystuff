package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.DocOption
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.{DataRow, FetchedRow, Field}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.{CommonUtils, Timeout}
import com.tribbloids.spookystuff.{ActionException, Const, SpookyEnvFixture}

import scala.collection.immutable.ListMap
import scala.concurrent.{duration, TimeoutException}
import scala.util.Random

class ActionSuite extends SpookyEnvFixture {

  import duration._
  import ActionSuite._

  it("interpolate should not change timeout") {
    import scala.concurrent.duration._

    val randomTimeout = Timeout(Random.nextInt().seconds)
    val action = Wget(Const.keyDelimiter + "{~}").in(randomTimeout)

    val rewritten = action
      .interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), emptySchema)
      .get

    //    val a = rewritten.uri.asInstanceOf[Literal[FR, String]].dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize
    //    val b = Literal("http://www.dummy.com").dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize
    //    val c = Literal(new Example()).dataType.asInstanceOf[UnreifiedScalaType].ttg.tpe.normalize

    assert(rewritten === Wget(Lit.erased("http://www.dummy.com")))
    assert(rewritten.timeout(null) === randomTimeout)
    assert(FilePaths.Hierarchical.apply(rewritten :: Nil) contains "/www.dummy.com")
  }

  it("interpolate should not change name") {

    val action = Wget("'{~}").as('dummy_name)

    val rewritten = action
      .interpolate(FetchedRow(DataRow(data = ListMap(Field("~") -> "http://www.dummy.com")), Seq()), emptySchema)
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

    it("-> memberStrPretty") {
      val str = action.memberStrPretty
      str.shouldBe(
        """
          |Wget(
          |	http://dummy.com,
          |	MustHaveTitle
          |)
        """.stripMargin
      )
    }
  }

  it("Timed mixin can terminate execution if it takes too long") {

    val a = OverdueExport
    val session = new Session(this.spooky)
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

  case object OverdueExport extends Export with Timed {

    override def doExeNoName(session: Session): Seq[DocOption] = {
      Thread.sleep(120 * 1000)
      Nil
    }
  }

}
