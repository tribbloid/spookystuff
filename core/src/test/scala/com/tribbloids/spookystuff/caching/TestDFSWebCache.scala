package com.tribbloids.spookystuff.caching

/**
  * Created by peng on 25/06/16.
  */
class TestDFSWebCache extends TestInMemoryWebCache {

  override lazy val cache: AbstractWebCache = DFSWebCache
}
