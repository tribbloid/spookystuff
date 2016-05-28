package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.caching.InMemoryWebCache
import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.{SpookyContext, dsl}

object LazyDocs {

  def apply(
             actions: Array[Action] = Array(),
             docs: Seq[Fetched] = null //override, cannot be shuffled
           ) = new LazyDocs(actions, docs)

}

class LazyDocs(
                actions: Array[Action] = Array(),
                @transient @volatile var docs: Seq[Fetched] = null //override, cannot be shuffled
              ) extends Serializable {

  import dsl._

  @transient lazy val trace = actions.toList

  class W(spooky: SpookyContext) {
    private def getCached: Option[Seq[Fetched]] = Option(docs).orElse {
      InMemoryWebCache.get(trace, spooky)
    }

    //fetched may yield very large documents and should only be loaded lazily and not shuffled or persisted (unless in-memory)
    def get(): Seq[Fetched] = {
      getCached.getOrElse{
        refresh()
      }
    }

    def refresh(): Seq[Fetched] = {
      val docs = trace.fetch(spooky)
      put(docs)
      docs
    }

    def put(docs: Seq[Fetched]): this.type = {
      LazyDocs.this.docs = docs
      InMemoryWebCache.put(trace, docs, spooky)
      this
    }
  }
}
