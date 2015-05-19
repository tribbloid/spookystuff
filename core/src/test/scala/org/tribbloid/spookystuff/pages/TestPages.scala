package org.tribbloid.spookystuff.pages

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.actions.{Snapshot, Visit, Wget}
import org.tribbloid.spookystuff.dsl._

import scala.concurrent.duration._

/**
 * Created by peng on 10/17/14.
 */
class TestPages extends SpookyEnvSuite {

  lazy val page = (Visit("http://en.wikipedia.org")::Snapshot().as('old)::Nil).resolve(spooky).map(_.asInstanceOf[Page])
  lazy val wgetPage = (Wget("http://en.wikipedia.org").as('oldWget)::Nil).resolve(spooky).map(_.asInstanceOf[Page])

  test("cache and restore") {
    spooky.conf.pageExpireAfter = 2.seconds

    assert(page.head.uid === PageUID(Visit("http://en.wikipedia.org") :: Snapshot().as('U) :: Nil, Snapshot()))

    PageUtils.autoCache(page, spooky)

    val loadedPages = PageUtils.autoRestore(page.head.uid.backtrace,spooky).map(_.asInstanceOf[Page])

    assert(loadedPages.length === 1)
    assert(page.head.content === loadedPages.head.content)
    assert(page.head.copy(content = null) === loadedPages.head.copy(content = null))
  }

  test ("local cache") {
    spooky.conf.pageExpireAfter = 2.seconds

    PageUtils.autoCache(page, spooky)

    val newTrace = Visit("http://en.wikipedia.org") :: Snapshot().as('new) :: Nil

    val page2 = PageUtils.autoRestore(newTrace, spooky).map(_.asInstanceOf[Page])

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.code === page2.head.code)
    assert(page2.head.name === "new")

    Thread.sleep(2000)

    val page3 = PageUtils.autoRestore(page.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.conf.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === page.head.copy(content = null))
    assert(page2.head.code === page2.head.code)
  }

  test ("wget local cache") {
    spooky.conf.pageExpireAfter = 2.seconds

    PageUtils.autoCache(wgetPage, spooky)

    val newTrace = Wget("http://en.wikipedia.org").as('newWget) :: Nil

    val page2 = PageUtils.autoRestore(newTrace, spooky).map(_.asInstanceOf[Page])

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === wgetPage.head.copy(content = null))
    assert(page2.head.code === page2.head.code)
    assert(page2.head.name === "newWget")

    Thread.sleep(2000)

    val page3 = PageUtils.autoRestore(wgetPage.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.conf.pageExpireAfter = 30.days

    assert(page2.size === 1)
    assert(page2.head.copy(content = null) === wgetPage.head.copy(content = null))
    assert(page2.head.code === page2.head.code)
  }

//  test ("s3 cache") {
//
//    spooky.setRoot(s"s3a://spooky-unit/")
//    spooky.pageExpireAfter = 10.seconds
//
//    Pages.autoCache(page, page.head.uid.backtrace, spooky)
//
//    val newTrace = Trace(Visit("http://en.wikipedia.org")::Snapshot().as('new)::Nil)
//
//    val page2 = Pages.autoRestoreLatest(newTrace, spooky)
//
//    assert(page2.size === 1)
//    assert(page2.head.copy(content = null) === page.head.copy(content = null))
//    assert(page2.head.markup === page2.head.markup)
//    assert(page2.head.name === "new")
//
//    Thread.sleep(12000)
//
//    val page3 = Pages.autoRestoreLatest(page.head.uid.backtrace, spooky)
//    assert(page3 === null)
//
//    spooky.pageExpireAfter = 30.days
//
//    assert(page2.size === 1)
//    assert(page2.head.copy(content = null) === page.head.copy(content = null))
//    assert(page2.head.markup === page2.head.markup)
//  }

}
