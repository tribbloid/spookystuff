package com.tribbloids.spookystuff.utils.io

object LocalResolverSuite {

  val nonExisting = s"not-a-file.txt"
  val nonExistingRelative = "non-existing/not-a-file.txt"
  val nonExistingAbsolute = "/non-existing/not-a-file.txt"
}

/**
  * Created by peng on 07/10/15.
  */
class LocalResolverSuite extends AbstractURIResolverSuite {

  @transient lazy val resolver: URIResolver = LocalResolver
  @transient lazy val schemaPrefix = ""
}
