package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.ControlBlock.{LocalRetry, Loop}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

import java.util.Date

class ControlBlockSpec extends SpookyBaseSpec with FileDocsFixture {

  import scala.concurrent.duration.*

  it("loop without export won't need driver") {

    val loop = Loop(
      Delay(1.seconds) +> Wget(HTML_URL)
    )

    val agent = new Agent(
      this.spooky
    )
    loop.exe(agent)

    assert(agent.getDriver.lookup.isEmpty)
//    assert(!loop.needDriver)
  }

  it("LocalRetry without export won't need driver") {
    import scala.concurrent.duration.*

    val retry = LocalRetry(
      Delay(1.seconds) +> Wget(HTML_URL)
    )

    val agent = new Agent(
      this.spooky
    )
    retry.exe(agent)

    assert(agent.getDriver.lookup.isEmpty)
  }

  it("LocalRetry will fail on malformed uri") {

    import scala.concurrent.duration.*

    val retry = LocalRetry(
      Delay(1.seconds) +> Wget(HTML_URL)
    )

    val agent = new Agent(
      this.spooky
    )
    retry.exe(agent)
  }

  it("wayback time of loop should be identical to its last child supporting wayback") {
    val waybackDate = new Date()

    val loop = Loop(
      Delay(1.seconds) +> Wget("ftp://www.dummy.co").waybackTo(waybackDate)
        +> Delay(20.seconds) +> Wget("ftp://www.dummy2.co").waybackToTimeMillis(waybackDate.getTime - 100000)
    )

    assert(loop.wayback.contains(waybackDate.getTime - 100000))
  }
}
