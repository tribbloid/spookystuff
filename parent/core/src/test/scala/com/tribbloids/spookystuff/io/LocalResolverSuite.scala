package com.tribbloids.spookystuff.io

object LocalResolverSuite {

  val nonExisting: String = s"not-a-file.txt"
  val nonExistingRelative: String = "non-existing/not-a-file.txt"
  val nonExistingAbsolute: String = "/non-existing/not-a-file.txt"
}

/**
  * Created by peng on 07/10/15.
  */
class LocalResolverSuite extends AbstractURIResolverSuite {

  @transient lazy val resolver: URIResolver = LocalResolver
  @transient lazy val schemaPrefix: String = ""
}
