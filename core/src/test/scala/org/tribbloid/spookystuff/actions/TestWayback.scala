package org.tribbloid.spookystuff.actions

import java.util.Date

import org.tribbloid.spookystuff.{RemoteDisabledException, SpookyEnvSuite}

/**
 * Created by peng on 08/09/15.
 */
class TestWayback extends SpookyEnvSuite {

  import org.tribbloid.spookystuff.dsl._

  import scala.concurrent.duration._

  test("Wget.waybackTo should work on cache") {
    spooky.conf.cacheWrite = true
    spooky.conf.pageNotExpiredSince = Some(new Date())

    val dates: Seq[Long] = (0 to 2).toSeq.map {
      i =>
        val pages = (Delay(5.seconds) +> Wget("http://www.wikipedia.org")).head.resolve(spooky) //5s is long enough
        assert(pages.size == 1)
        pages.head.timestamp.getTime
    }

    spooky.conf.cacheRead = true

    val cachedPages = (Delay(5.seconds)
        +> Wget("http://www.wikipedia.org").waybackToTimeMillis(dates(1) + 2000)
      ).head.resolve(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timestamp.getTime == dates(1))

    spooky.conf.remote = false

    intercept[RemoteDisabledException] {
      (Delay(5.seconds)
        +> Wget("http://www.wikipedia.org").waybackToTimeMillis(dates.head - 2000)
        ).head.resolve(spooky)
    }
  }

  test("Snapshot.waybackTo should work on cache") {
    spooky.conf.cacheWrite = true
    spooky.conf.pageNotExpiredSince = Some(new Date())

    val dates: Seq[Long] = (0 to 2).toSeq.map {
      i =>
        val pages = (Delay(5.seconds)
          +> Visit("http://www.wikipedia.org")).autoSnapshot.head.resolve(spooky) //5s is long enough
        assert(pages.size == 1)
        pages.head.timestamp.getTime
    }

    spooky.conf.cacheRead = true

    val cachedPages = (Delay(5.seconds)
      +> Visit("http://www.wikipedia.org")
      +> Snapshot().waybackToTimeMillis(dates(1) + 2000)
      ).head.resolve(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timestamp.getTime == dates(1))

    spooky.conf.remote = false

    intercept[RemoteDisabledException] {
      (Delay(5.seconds)
        +> Visit("http://www.wikipedia.org")
        +> Snapshot().waybackToTimeMillis(dates.head - 2000)
        ).head.resolve(spooky)
    }
  }

  test("Screenshot.waybackTo should work on cache") {
    spooky.conf.cacheWrite = true
    spooky.conf.pageNotExpiredSince = Some(new Date())

    val dates: Seq[Long] = (0 to 2).toSeq.map {
      i =>
        val pages = (Delay(5.seconds)
          +> Visit("http://www.wikipedia.org")
          +> Screenshot()).head.resolve(spooky) //5s is long enough
        assert(pages.size == 1)
        pages.head.timestamp.getTime
    }

    spooky.conf.cacheRead = true

    val cachedPages = (Delay(5.seconds)
      +> Visit("http://www.wikipedia.org")
      +> Screenshot().waybackToTimeMillis(dates(1) + 2000)
      ).head.resolve(spooky)
    assert(cachedPages.size == 1)
    assert(cachedPages.head.timestamp.getTime == dates(1))

    spooky.conf.remote = false

    intercept[RemoteDisabledException] {
      (Delay(5.seconds)
        +> Visit("http://www.wikipedia.org")
        +> Screenshot().waybackToTimeMillis(dates.head - 2000)
        ).head.resolve(spooky)
    }
  }
}
