package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.{Delay, Wget}

import java.util.Date
import com.tribbloids.spookystuff.{QueryException, SpookyEnvFixture}

/**
  * Created by peng on 08/09/15.
  */
//TODO: test independently for each cache type (after switch for different cache is implemented)
class TestWayback extends SpookyEnvFixture {


  import scala.concurrent.duration._

  it("Wget.waybackTo should work on cache") {
    spooky.spookyConf.cacheWrite = true
    spooky.spookyConf.IgnoreCachedDocsBefore = Some(new Date())

    val dates: Seq[Long] = (0 to 2).map { i =>
      val pages = (Delay(10.seconds) +> Wget(HTML_URL)).fetch(spooky) //5s is long enough
      assert(pages.size == 1)
      pages.head.timeMillis
    }

    spooky.spookyConf.cacheRead = true

    val cachedPages = (Delay(10.seconds)
      +> Wget(HTML_URL).waybackToTimeMillis(dates(1) + 2000)).fetch(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timeMillis == dates(1))

    spooky.spookyConf.remote = false

    intercept[QueryException] {
      (Delay(10.seconds)
        +> Wget(HTML_URL).waybackToTimeMillis(dates.head - 2000)).fetch(spooky)
    }
  }

  it("Snapshot.waybackTo should work on cache") {
    spooky.spookyConf.cacheWrite = true
    spooky.spookyConf.IgnoreCachedDocsBefore = Some(new Date())

    val dates: Seq[Long] = (0 to 2).map { i =>
      val pages = (
        Delay(10.seconds)
          +> Visit(HTML_URL)
      ).rewriteGlobally(defaultSchema).head.fetch(spooky) //5s is long enough
      assert(pages.size == 1)
      pages.head.timeMillis
    }

    spooky.spookyConf.cacheRead = true

    val cachedPages = (Delay(10.seconds)
      +> Visit(HTML_URL)
      +> Snapshot().waybackToTimeMillis(dates(1) + 2000)).fetch(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timeMillis == dates(1))

    spooky.spookyConf.remote = false

    intercept[QueryException] {
      (Delay(10.seconds)
        +> Visit(HTML_URL)
        +> Snapshot().waybackToTimeMillis(dates.head - 2000)).fetch(spooky)
    }
  }

  it("Screenshot.waybackTo should work on cache") {
    spooky.spookyConf.cacheWrite = true
    spooky.spookyConf.IgnoreCachedDocsBefore = Some(new Date())

    val dates: Seq[Long] = (0 to 2).map { i =>
      val pages = (Delay(10.seconds)
        +> Visit(HTML_URL)
        +> Screenshot()).fetch(spooky) //5s is long enough
      assert(pages.size == 1)
      pages.head.timeMillis
    }

    spooky.spookyConf.cacheRead = true

    val cachedPages = (Delay(10.seconds)
      +> Visit(HTML_URL)
      +> Screenshot().waybackToTimeMillis(dates(1) + 2000)).fetch(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timeMillis == dates(1))

    spooky.spookyConf.remote = false

    intercept[QueryException] {
      (Delay(10.seconds)
        +> Visit(HTML_URL)
        +> Screenshot().waybackToTimeMillis(dates.head - 2000)).fetch(spooky)
    }
  }
}
