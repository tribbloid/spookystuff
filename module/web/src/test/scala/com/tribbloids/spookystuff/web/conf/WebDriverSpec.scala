package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

object WebDriverSpec {

  object Chrome extends WebDriverCase {
    override lazy val transientDriverFactory =
      WebDriverFactory.Chrome()
  }

  object Firefox extends WebDriverCase {
    override lazy val transientDriverFactory =
      WebDriverFactory.Firefox()
  }

}

class WebDriverSpec extends SpookyBaseSpec with FileDocsFixture {

  import WebDriverSpec.*

  override lazy val nestedSuites = {

    IndexedSeq(
      Chrome,
      Firefox
    )
  }
}
