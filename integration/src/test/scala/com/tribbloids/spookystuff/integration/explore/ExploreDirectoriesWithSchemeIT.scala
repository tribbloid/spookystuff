package com.tribbloids.spookystuff.integration.explore


class ExploreDirectoriesWithSchemeIT extends ExploreDirectoriesIT {

  override lazy val resourcePath: String = "file:" + _resourcePath
}
