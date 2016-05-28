package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.actions.{Snapshot, Visit, Wget}
import com.tribbloids.spookystuff.dsl._

import scala.concurrent.duration._

/**
 * Created by peng on 10/17/14.
 */
class TestPages extends SpookyEnvSuite {

  lazy val page = (Visit("http://en.wikipedia.org")::Snapshot().as('old)::Nil).fetch(spooky).map(_.asInstanceOf[Doc])
  lazy val wgetPage = (Wget("http://en.wikipedia.org").as('oldWget)::Nil).fetch(spooky).map(_.asInstanceOf[Doc])

  test("cache and restore") {
    spooky.conf.docsLifeSpan = 2.seconds

    assert(page.head.uid === DocUID(Visit("http://en.wikipedia.org") :: Snapshot().as('U) :: Nil, Snapshot()))

    DocUtils.autoCache(page, spooky)

    val loadedPages = DocUtils.autoRestore(page.head.uid.backtrace,spooky).map(_.asInstanceOf[Doc])

    assert(loadedPages.length === 1)
    assert(page.head.content === loadedPages.head.content)
    assert(page.head === loadedPages.head)
  }

  test ("local cache") {
    spooky.conf.docsLifeSpan = 5.seconds

    DocUtils.autoCache(page, spooky)

    val newTrace = Visit("http://en.wikipedia.org") :: Snapshot().as('new) :: Nil

    val page2 = DocUtils.autoRestore(newTrace, spooky).map(_.asInstanceOf[Doc])

    assert(page2.size === 1)
    assert(page2.head === page.head)
    assert(page2.head.code === page2.head.code)
    assert(page2.head.name === "new")

    Thread.sleep(5000)

    val page3 = DocUtils.autoRestore(page.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.conf.docsLifeSpan = 30.days

    assert(page2.size === 1)
    assert(page2.head === page.head)
    assert(page2.head.code === page2.head.code)
  }

  test ("wget local cache") {
    spooky.conf.docsLifeSpan = 2.seconds

    DocUtils.autoCache(wgetPage, spooky)

    val newTrace = Wget("http://en.wikipedia.org").as('newWget) :: Nil

    val page2 = DocUtils.autoRestore(newTrace, spooky).map(_.asInstanceOf[Doc])

    assert(page2.size === 1)
    assert(page2.head === wgetPage.head)
//    assert(page2.head.code === page2.head.code)
    assert(page2.head.name === "newWget")

    Thread.sleep(2000)

    val page3 = DocUtils.autoRestore(wgetPage.head.uid.backtrace, spooky)
    assert(page3 === null)

    spooky.conf.docsLifeSpan = 30.days

    assert(page2.size === 1)
    assert(page2.head === wgetPage.head)
//    assert(page2.head.code === page2.head.code)
  }

//  test ("s3 cache") {
//
//    spooky.setRoot(s"s3n://spooky-unit/")
//    spooky.pageExpireAfter = 10.seconds
//
//    Pages.autoCache(page, page.head.uid.backtrace, spooky)
//
//    val newTrace = Trace(Visit("http://en.wikipedia.org")::Snapshot().as('new)::Nil)
//
//    val page2 = Pages.autoRestoreLatest(newTrace, spooky)
//
//    assert(page2.size === 1)
//    assert(page2.head === page.head)
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
//    assert(page2.head === page.head)
//    assert(page2.head.markup === page2.head.markup)
//  }

}
