package com.tribbloids.spookystuff.utils.io

class LocalResolverSuite extends AbstractURIResolverSuite {

  override val resolver: URIResolver = LocalResolver
  override val schemaPrefix: String = ""
}
