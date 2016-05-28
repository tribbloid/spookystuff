package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Fetched

/**
  * Created by peng on 07/06/16.
  */
trait AbstractWebCache {

  def get(k: Trace, spooky: SpookyContext) = getImpl(k, spooky)
  def getImpl(k: Trace, spooky: SpookyContext): Option[Seq[Fetched]]

  def put(k: Trace, v: Seq[Fetched], spooky: SpookyContext) = putImpl(k, v, spooky)
  def putImpl(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type

  def putIfAbsent(k: Trace, v: Seq[Fetched], spooky: SpookyContext) = putIfAbsentImpl(k, v, spooky)
  def putIfAbsentImpl(k: Trace, v: Seq[Fetched], spooky: SpookyContext): this.type
}
