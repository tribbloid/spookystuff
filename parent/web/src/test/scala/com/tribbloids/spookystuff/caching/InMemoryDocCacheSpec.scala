package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.actions.{Trace, Wget}
import com.tribbloids.spookystuff.conf.Core
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.web.actions.{Snapshot, Visit}

import scala.concurrent.duration._

/**
  * Created by peng on 10/17/14.
  */
class InMemoryDocCacheSpec extends SpookyBaseSpec with FileDocsFixture {

  lazy val cache: AbstractDocCache = InMemoryDocCache

  val visit: Trace = Visit(HTML_URL) +> Snapshot().as('old)
  def visitPage: Seq[Doc] = visit.fetch(spooky).map(_.asInstanceOf[Doc])

  val wget: Wget = Wget(HTML_URL).as('oldWget)
  def wgetPage: Seq[Doc] =
    wget
      .fetch(spooky)
      .map(
        _.asInstanceOf[Doc].updated(cacheLevel = DocCacheLevel.All)
      ) // By default wget from DFS are only cached in-memory

  lazy val shortLifeSpan: FiniteDuration = 15.seconds

  it("cache and restore") {
    val doc = this.visitPage

    spooky(Core).confUpdate(_.copy(cachedDocsLifeSpan = shortLifeSpan))

    assert(doc.head.uid === DocUID(Visit(HTML_URL) :: Snapshot().as('U) :: Nil, Snapshot())())

    cache.put(visit, doc, spooky)

    val doc2 = cache.get(doc.head.uid.backtrace, spooky).get.map(_.asInstanceOf[Doc])

    assert(doc2.length === 1)

    {
      val docs = Seq(doc, doc2)
      docs.map(_.head.samenessDelegatedTo.toString).shouldBeIdentical()
      docs.map(_.head.content.contentStr).shouldBeIdentical()
      docs.map(_.head.code.toString).shouldBeIdentical()
    }

    assert(doc.head === doc2.head)
  }

  it("cache visit and restore with different name") {
    val doc = this.visitPage

    spooky(Core).confUpdate(_.copy(cachedDocsLifeSpan = shortLifeSpan))

    cache.put(visit, doc, spooky)

    val newTrace = Visit(HTML_URL) +> Snapshot().as('new)

    Thread.sleep(1000)
    val doc2 = cache.get(newTrace, spooky).get.map(_.asInstanceOf[Doc])

    assert(doc2.size === 1)

    {
      val docs = Seq(doc, doc2)
      docs.map(_.head.samenessDelegatedTo.toString).shouldBeIdentical()
      docs.map(_.head.content.contentStr).shouldBeIdentical()
      docs.map(_.head.code.toString).shouldBeIdentical()
    }

    assert(doc2.head.name === "new")

    Thread.sleep(shortLifeSpan.toMillis)

    val doc3 = cache.get(doc.head.uid.backtrace, spooky).orNull
    assert(doc3 === null)

    spooky(Core).confUpdate(_.copy(cachedDocsLifeSpan = 30.days))

    val doc4 = cache.get(newTrace, spooky).get.map(_.asInstanceOf[Doc])
    assert(doc4.size === 1)

    {
      val docs = Seq(doc, doc4)
      docs.map(_.head.samenessDelegatedTo.toString).shouldBeIdentical()
      docs.map(_.head.content.contentStr).shouldBeIdentical()
      docs.map(_.head.code.toString).shouldBeIdentical()
    }
  }

  it("cache wget and restore with different name") {
    val doc = this.wgetPage
    spooky(Core).confUpdate(_.copy(cachedDocsLifeSpan = shortLifeSpan))

    cache.put(wget, doc, spooky)

    val newTrace = Wget(HTML_URL).as('newWget) :: Nil

    Thread.sleep(1000)
    val doc2 = cache.get(newTrace, spooky).get.map(_.asInstanceOf[Doc])

    assert(doc2.size === 1)

    {
      val docs = Seq(doc, doc2)
      docs.map(_.head.samenessDelegatedTo.toString).shouldBeIdentical()
      docs.map(_.head.content.contentStr).shouldBeIdentical()
      docs.map(_.head.code.toString).shouldBeIdentical()
    }

    Thread.sleep(shortLifeSpan.toMillis)

    val doc3 = cache.get(doc.head.uid.backtrace, spooky).orNull
    assert(doc3 === null)

    spooky(Core).confUpdate(_.copy(cachedDocsLifeSpan = 30.days))

    val doc4 = cache.get(newTrace, spooky).get.map(_.asInstanceOf[Doc])
    assert(doc4.size === 1)

    {
      val docs = Seq(doc, doc4)
      docs.map(_.head.samenessDelegatedTo.toString).shouldBeIdentical()
      docs.map(_.head.content.contentStr).shouldBeIdentical()
      docs.map(_.head.code.toString).shouldBeIdentical()
    }
  }

  // TODO: test trace, block and more complex cases
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
