package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec

import java.util.Date

class BlockSpec extends SpookyBaseSpec {

  import scala.concurrent.duration._

  it("loop without export won't need driver") {

    val loop = Loop(
      Delay(10.seconds) +> Wget("ftp://www.dummy.co")
    )

    val session = new Agent(
      this.spooky
    )
    loop.exe(session)

    assert(session.Drivers.lookup.isEmpty)
//    assert(!loop.needDriver)
  }

  it("try without export won't need driver") {
    import scala.concurrent.duration._

    val tryy = ClusterRetry(
      Delay(10.seconds) +> Wget("ftp://www.dummy.org")
    )

    val session = new Agent(
      this.spooky
    )
    tryy.exe(session)

    assert(session.Drivers.lookup.isEmpty)
  }

  it("Try(Wget) can failsafe on malformed uri") {}

  it("wayback time of loop should be identical to its last child supporting wayback") {
    val waybackDate = new Date()

    val loop = Loop(
      Delay(10.seconds) +> Wget("ftp://www.dummy.co").waybackTo(waybackDate)
        +> Delay(20.seconds) +> Wget("ftp://www.dummy2.co").waybackToTimeMillis(waybackDate.getTime - 100000)
    )

    assert(loop.wayback == Lit[Long](waybackDate.getTime - 100000))
  }
}
