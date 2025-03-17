package com.tribbloids.spookystuff.caching

/**
  * Created by peng on 25/06/16.
  */
class DFSDocCacheSpec extends InMemoryDocCacheSpec {

  override lazy val cache: AbstractDocCache = DFSDocCache
}
