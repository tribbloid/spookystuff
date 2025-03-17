package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.actions.{Trace, Wget}
import com.tribbloids.spookystuff.conf.Core
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

import scala.concurrent.duration.*

/**
  * Created by peng on 10/17/14.
  */
class InMemoryDocCacheSpec extends SpookyBaseSpec with FileDocsFixture {

  lazy val cache: AbstractDocCache = InMemoryDocCache

  val action = Wget(HTML_URL).as("old")
  def execute: Seq[Doc] = {
    val fetched = action
      .fetch(spooky)

    fetched
      .map(
        _.asInstanceOf[Doc].updated(cacheLevel = DocCacheLevel.All)
      )
  } // By default wget from DFS are only cached in-memory

  lazy val shortLifeSpan: FiniteDuration = 15.seconds

  describe("cache and restore") {

    it("with no name") {

      val doc = this.execute

      spooky(Core).confUpdate(_.copy(cachedDocsLifeSpan = shortLifeSpan))

      assert(doc.head.uid === DocUID(Wget(HTML_URL) :: Nil, Wget(HTML_URL))())

      cache.put(action, doc, spooky)

      val doc2 = cache.get(doc.head.uid.backtrace, spooky).get.map(_.asInstanceOf[Doc])

      assert(doc2.length === 1)

      {
        val docs = Seq(doc, doc2)
        docs.map(_.head.samenessKey.toString).shouldBeIdentical()
        docs.map(_.head.content.contentStr).shouldBeIdentical()
        docs.map(_.head.code.toString).shouldBeIdentical()
      }

      assert(doc.head === doc2.head)
    }

    it("with a different name") {
      val doc = this.execute
      spooky(Core).confUpdate(_.copy(cachedDocsLifeSpan = shortLifeSpan))

      cache.put(action, doc, spooky)

      val newTrace = Wget(HTML_URL).as("new") :: Nil

      Thread.sleep(1000)
      val doc2 = cache.get(Trace(newTrace), spooky).get.map(_.asInstanceOf[Doc])

      assert(doc2.size === 1)
      assert(doc2.head.name === "new")

      {
        val docs = Seq(doc, doc2)
        docs.map(_.head.samenessKey.toString).shouldBeIdentical()
        docs.map(_.head.content.contentStr).shouldBeIdentical()
        docs.map(_.head.code.toString).shouldBeIdentical()
      }

      Thread.sleep(shortLifeSpan.toMillis)

      val doc3 = cache.get(doc.head.uid.backtrace, spooky).orNull
      assert(doc3 === null)

      spooky(Core).confUpdate(_.copy(cachedDocsLifeSpan = 30.days))

      val doc4 = cache.get(Trace(newTrace), spooky).get.map(_.asInstanceOf[Doc])
      assert(doc4.size === 1)

      {
        val docs = Seq(doc, doc4)
        docs.map(_.head.samenessKey.toString).shouldBeIdentical()
        docs.map(_.head.content.contentStr).shouldBeIdentical()
        docs.map(_.head.code.toString).shouldBeIdentical()
      }
    }
  }

}
