package org.tribbloid.spookystuff.pages

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.actions.{Snapshot, Trace, Visit, Wget}
import org.tribbloid.spookystuff.dsl._

import scala.concurrent.duration._

/**
 * Created by peng on 10/17/14.
 */
class TestPages extends SpookyEnvSuite {

  val page = Trace(Visit("http://en.wikipedia.org")::Snapshot().as('old)::Nil).resolve(spooky)

  val wgetPage = Trace(Wget("http://en.wikipedia.org").as('oldWget)::Nil).resolve(spooky)

  test("cache and restore") {
    spooky.pageExpireAfter = 2.seconds

    val pages = Trace(
      Visit("http://en.wikipedia.org") ::
        Snapshot().as('T) :: Nil
    ).resolve(spooky)

    assert(pages.size === 1)
    val page1 = pages(0)

    assert(page1.uid === PageUID(Trace(Visit("http://en.wikipedia.org") :: Snapshot().as('U) :: Nil), Snapshot()))

    Pages.autoCache(pages, page1.uid.backtrace, spooky)

    val loadedPages = Pages.autoRestoreLatest(page1.uid.backtrace,spooky)

    assert(loadedPages.length === 1)
    assert(pages(0).content === loadedPages(0).content)
    assert(pages(0).copy(content = null) === loadedPages(0).copy(content = null))
  }

  test ("local cache") {
    spooky.pageExpireAfter = 2.seconds

    Pages.autoCache(page, page.head.uid.backtrace, spooky)

    val newTrace = Trace(Visit("http://en.wikipedia.org")::Snapshot().as('new)::Nil)

    val page2 = Pages.autoRestoreLatest(newTrace, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.markup === page2.head.markup)
    assert(page2.head.name === "new")

    Thread.sleep(2000)

    val page3 = Pages.autoRestoreLatest(page.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.markup === page2.head.markup)
  }

  test ("wget local cache") {
    spooky.pageExpireAfter = 2.seconds

    Pages.autoCache(wgetPage, wgetPage.head.uid.backtrace, spooky)

    val newTrace = Trace(Wget("http://en.wikipedia.org").as('newWget)::Nil)

    val page2 = Pages.autoRestoreLatest(newTrace, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === wgetPage.head.copy(content = null))
    assert(page2.head.markup === page2.head.markup)
    assert(page2.head.name === "newWget")

    Thread.sleep(2000)

    val page3 = Pages.autoRestoreLatest(wgetPage.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === wgetPage.head.copy(content = null))
    assert(page2.head.markup === page2.head.markup)
  }

  test ("s3 cache") {

    spooky.setRoot(s"s3n://spooky-unit/")
    spooky.pageExpireAfter = 10.seconds

    Pages.autoCache(page, page.head.uid.backtrace, spooky)

    val newTrace = Trace(Visit("http://en.wikipedia.org")::Snapshot().as('new)::Nil)

    val page2 = Pages.autoRestoreLatest(newTrace, spooky)

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.markup === page2.head.markup)
    assert(page2.head.name === "new")

    Thread.sleep(12000)

    val page3 = Pages.autoRestoreLatest(page.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.markup === page2.head.markup)
  }

}
