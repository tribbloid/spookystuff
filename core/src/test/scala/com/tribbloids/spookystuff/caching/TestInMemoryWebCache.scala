package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.{Snapshot, Visit, Wget}
import com.tribbloids.spookystuff.doc.{Doc, DocUID}
import com.tribbloids.spookystuff.dsl._

import scala.concurrent.duration._

/**
 * Created by peng on 10/17/14.
 */
class TestInMemoryWebCache extends SpookyEnvFixture {

  lazy val cache: AbstractWebCache = InMemoryWebCache

  val visit = Visit("http://en.wikipedia.org")::Snapshot().as('old)::Nil
  lazy val visitPage = visit.fetch(spooky).map(_.asInstanceOf[Doc])

  val wget = Wget("http://en.wikipedia.org").as('oldWget)::Nil
  lazy val wgetPage = wget.fetch(spooky).map(_.asInstanceOf[Doc])

  test("cache and restore") {
    spooky.conf.docsLifeSpan = 2.seconds

    assert(visitPage.head.uid === DocUID(Visit("http://en.wikipedia.org") :: Snapshot().as('U) :: Nil, Snapshot()))

    cache.put(visit, visitPage, spooky)

    val loadedPages = cache.get(visitPage.head.uid.backtrace,spooky).get.map(_.asInstanceOf[Doc])

    assert(loadedPages.length === 1)
    assert(visitPage.head.content === loadedPages.head.content)
    assert(visitPage.head === loadedPages.head)
  }

  test ("cache visit and restore with different name") {
    spooky.conf.docsLifeSpan = 5.seconds

    cache.put(visit, visitPage, spooky)

    val newTrace = Visit("http://en.wikipedia.org") :: Snapshot().as('new) :: Nil

    val page2 = cache.get(newTrace, spooky).get.map(_.asInstanceOf[Doc])

    assert(page2.size === 1)
    assert(page2.head === visitPage.head)
    assert(page2.head.code === page2.head.code)
    assert(page2.head.name === "new")

    Thread.sleep(5000)

    val page3 = cache.get(visitPage.head.uid.backtrace, spooky).orNull
    assert(page3 === null)

    spooky.conf.docsLifeSpan = 30.days

    assert(page2.size === 1)
    assert(page2.head === visitPage.head)
    assert(page2.head.code === page2.head.code)
  }

  test ("cache wget and restore with different name") {
    spooky.conf.docsLifeSpan = 2.seconds

    cache.put(wget, wgetPage, spooky)

    val newTrace = Wget("http://en.wikipedia.org").as('newWget) :: Nil

    val page2 = cache.get(newTrace, spooky).get.map(_.asInstanceOf[Doc])

    assert(page2.size === 1)
    assert(page2.head === wgetPage.head)
//    assert(page2.head.code === page2.head.code)
    assert(page2.head.name === "newWget")

    Thread.sleep(2000)

    val page3 = cache.get(wgetPage.head.uid.backtrace, spooky).orNull
    assert(page3 === null)

    spooky.conf.docsLifeSpan = 30.days

    assert(page2.size === 1)
    assert(page2.head === wgetPage.head)
//    assert(page2.head.code === page2.head.code)
  }

  //TODO: test trace, block and more complex cases
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
