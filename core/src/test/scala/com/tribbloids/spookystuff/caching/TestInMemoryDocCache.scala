package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.{Snapshot, Visit, Wget}
import com.tribbloids.spookystuff.doc.{Doc, DocUID}
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture

import scala.concurrent.duration._

/**
 * Created by peng on 10/17/14.
 */
class TestInMemoryDocCache extends SpookyEnvFixture with LocalPathDocsFixture {

  lazy val cache: AbstractDocCache = InMemoryDocCache

  val visit = Visit(HTML_URL)::Snapshot().as('old)::Nil
  def visitPage = visit.fetch(spooky).map(_.asInstanceOf[Doc])

  val wget = Wget(HTML_URL).as('oldWget)::Nil
  lazy val wgetPage = wget.fetch(spooky).map(_.asInstanceOf[Doc].updated(cacheLevel = CacheLevel.All)) //By default wget from DFS are only cached in-memory

  it("cache and restore") {
    spooky.spookyConf.cachedDocsLifeSpan = 10.seconds

    assert(visitPage.head.uid === DocUID(Visit(HTML_URL) :: Snapshot().as('U) :: Nil, Snapshot())())

    cache.put(visit, visitPage, spooky)

    val loadedPages = cache.get(visitPage.head.uid.backtrace,spooky).get.map(_.asInstanceOf[Doc])

    assert(loadedPages.length === 1)
    assert(visitPage.head.raw === loadedPages.head.raw)
    assert(visitPage.head === loadedPages.head)
  }

  it ("cache visit and restore with different name") {
    spooky.spookyConf.cachedDocsLifeSpan = 10.seconds

    cache.put(visit, visitPage, spooky)

    val newTrace = Visit(HTML_URL) :: Snapshot().as('new) :: Nil

    val page2 = cache.get(newTrace, spooky).get.map(_.asInstanceOf[Doc])

    assert(page2.size === 1)
    assert(page2.head === visitPage.head)
    assert(page2.head.code === page2.head.code)
    assert(page2.head.name === "new")

    Thread.sleep(11000)

    val page3 = cache.get(visitPage.head.uid.backtrace, spooky).orNull
    assert(page3 === null)

    spooky.spookyConf.cachedDocsLifeSpan = 30.days

    assert(page2.size === 1)
    assert(page2.head === visitPage.head)
    assert(page2.head.code === page2.head.code)
  }

  it ("cache wget and restore with different name") {
    spooky.spookyConf.cachedDocsLifeSpan = 10.seconds

    cache.put(wget, wgetPage, spooky)

    val newTrace = Wget(HTML_URL).as('newWget) :: Nil

    val page2 = cache.get(newTrace, spooky).get.map(_.asInstanceOf[Doc])

    assert(page2.size === 1)
    assert(page2.head === wgetPage.head)
//    assert(page2.head.code === page2.head.code)
    assert(page2.head.name === "newWget")

    Thread.sleep(11000)

    val page3 = cache.get(wgetPage.head.uid.backtrace, spooky).orNull
    assert(page3 === null)

    spooky.spookyConf.cachedDocsLifeSpan = 30.days

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
//    val newTrace = Trace(Visit(HTML_URL)::Snapshot().as('new)::Nil)
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
