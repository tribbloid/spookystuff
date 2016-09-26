package com.tribbloids.spookystuff.actions

import java.util.Date

import com.tribbloids.spookystuff.{QueryException, SpookyEnvFixture}

/**
  * Created by peng on 08/09/15.
  */
//TODO: test independently for each cache type (after switch for different cache is implemented)
class TestWayback extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.dsl._

  import scala.concurrent.duration._

  test("Wget.waybackTo should work on cache") {
    spooky.conf.cacheWrite = true
    spooky.conf.IgnoreCachedDocsBefore = Some(new Date())

    val dates: Seq[Long] = (0 to 2).map {
      i =>
        val pages = (Delay(5.seconds) +> Wget("http://www.wikipedia.org")
          ).head.fetch(spooky) //5s is long enough
        assert(pages.size == 1)
        pages.head.timeMillis
    }

    spooky.conf.cacheRead = true

    val cachedPages = (Delay(5.seconds)
      +> Wget("http://www.wikipedia.org").waybackToTimeMillis(dates(1) + 2000)
      ).head.fetch(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timeMillis == dates(1))

    spooky.conf.remote = false

    intercept[QueryException] {
      (Delay(5.seconds)
        +> Wget("http://www.wikipedia.org").waybackToTimeMillis(dates.head - 2000)
        ).head.fetch(spooky)
    }
  }

  test("Snapshot.waybackTo should work on cache") {
    spooky.conf.cacheWrite = true
    spooky.conf.IgnoreCachedDocsBefore = Some(new Date())

    val dates: Seq[Long] = (0 to 2).map {
      i =>
        val pages = (Delay(5.seconds)
          +> Visit("http://www.wikipedia.org")
          ).correct.head.fetch(spooky) //5s is long enough
        assert(pages.size == 1)
        pages.head.timeMillis
    }

    spooky.conf.cacheRead = true

    val cachedPages = (Delay(5.seconds)
      +> Visit("http://www.wikipedia.org")
      +> Snapshot().waybackToTimeMillis(dates(1) + 2000)
      ).head.fetch(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timeMillis == dates(1))

    spooky.conf.remote = false

    intercept[QueryException] {
      (Delay(5.seconds)
        +> Visit("http://www.wikipedia.org")
        +> Snapshot().waybackToTimeMillis(dates.head - 2000)
        ).head.fetch(spooky)
    }
  }

  test("Screenshot.waybackTo should work on cache") {
    spooky.conf.cacheWrite = true
    spooky.conf.IgnoreCachedDocsBefore = Some(new Date())

    val dates: Seq[Long] = (0 to 2).map {
      i =>
        val pages = (Delay(5.seconds)
          +> Visit("http://www.wikipedia.org")
          +> Screenshot()).head.fetch(spooky) //5s is long enough
        assert(pages.size == 1)
        pages.head.timeMillis
    }

    spooky.conf.cacheRead = true

    val cachedPages = (Delay(5.seconds)
      +> Visit("http://www.wikipedia.org")
      +> Screenshot().waybackToTimeMillis(dates(1) + 2000)
      ).head.fetch(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timeMillis == dates(1))

    spooky.conf.remote = false

    intercept[QueryException] {
      (Delay(5.seconds)
        +> Visit("http://www.wikipedia.org")
        +> Screenshot().waybackToTimeMillis(dates.head - 2000)
        ).head.fetch(spooky)
    }
  }
}
