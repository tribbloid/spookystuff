package com.tribbloids.spookystuff.actions

import java.util.Date

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.extractors.Literal
import com.tribbloids.spookystuff.session.DriverSession

class TestBlock extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.dsl._

  import scala.concurrent.duration._

  test("loop without export won't need driver") {

    val loop = Loop(
      Delay(10.seconds) +> Wget("ftp://www.dummy.co")
    )

    val session = new DriverSession(
      this.spooky
    )
    loop.exe(session)

    assert(session.webDriverOpt.isEmpty)
//    assert(!loop.needDriver)
  }

  test("try without export won't need driver") {
    import scala.concurrent.duration._

    val tryy = Try(
      Delay(10.seconds) +> Wget("ftp://www.dummy.org")
    )

    val session = new DriverSession(
      this.spooky
    )
    tryy.exe(session)

    assert(session.webDriverOpt.isEmpty)
  }

  test("Try(Wget) can failsafe on malformed uri") {

  }

  test("wayback time of loop should be identical to its last child supporting wayback") {
    val waybackDate = new Date()

    val loop = Loop(
      Delay(10.seconds) +> Wget("ftp://www.dummy.co").waybackTo(waybackDate)
    +> Delay(20.seconds) +> Wget("ftp://www.dummy2.co").waybackToTimeMillis(waybackDate.getTime - 100000)
    )

    assert(loop.wayback == Literal[Long](waybackDate.getTime - 100000))
  }
}
