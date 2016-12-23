package com.tribbloids.spookystuff.caching

/**
  * Created by peng on 25/06/16.
  */
class TestDFSDocCache extends TestInMemoryDocCache {

  override lazy val cache: AbstractDocCache = DFSDocCache
}
