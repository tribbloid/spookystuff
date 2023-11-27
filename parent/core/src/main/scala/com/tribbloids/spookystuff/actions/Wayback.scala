package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Wayback.WaybackLike
import com.tribbloids.spookystuff.extractors.Extractor
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}

import java.util.Date

object Wayback {

  trait WaybackLike {

    def wayback: Extractor[Long]
  }
}

trait Wayback extends WaybackLike {

  var wayback: Extractor[Long] = _

  def waybackTo(date: Extractor[Date]): this.type = {
    this.wayback = date.andMap(_.getTime)
    this
  }

  def waybackTo(date: Date): this.type = this.waybackTo(Lit(date))

  def waybackToTimeMillis(time: Extractor[Long]): this.type = {
    this.wayback = time
    this
  }

  def waybackToTimeMillis(date: Long): this.type = this.waybackToTimeMillis(Lit(date))

  // has to be used after copy
  protected def injectWayback(
      wayback: Extractor[Long],
      pageRow: FetchedRow,
      schema: SpookySchema
  ): Option[this.type] = {
    if (wayback == null) Some(this)
    else {
      val valueOpt = wayback.resolve(schema).lift(pageRow)
      valueOpt.map { v =>
        this.wayback = Lit.erased(v)
        this
      }
    }
  }
}
