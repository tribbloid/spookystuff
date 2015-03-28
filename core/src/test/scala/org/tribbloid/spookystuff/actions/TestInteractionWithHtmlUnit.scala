package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.dsl.HtmlUnitDriverFactory
import org.tribbloid.spookystuff.pages.Page

import scala.concurrent.duration._

/**
 * Created by peng on 2/19/15.
 */
class TestInteractionWithHtmlUnit extends TestInteraction {

  override lazy val driverFactory = HtmlUnitDriverFactory()

  import org.tribbloid.spookystuff.dsl._

  //current phantomjs is buggy and cannot succeed on these two, which is why they are here
  test("click should not double click") {
    spooky.conf.remoteResourceTimeout = 180.seconds

    try {
      val results = (Visit("https://ca.vwr.com/store/search?&pimId=582903")
        +> Paginate("a[title=Next]", delay = 2.second)).head.self.resolve(spooky)

      val numPages = results.head.asInstanceOf[Page].children("div.right a").size

      assert(results.size == numPages)
    }

    finally {
      spooky.conf.remoteResourceTimeout = 60.seconds
    }
  }

  test("dynamic paginate should returns right number of pages") {
    spooky.conf.remoteResourceTimeout = 180.seconds

    try {
      val results = (Visit("https://ca.vwr.com/store/search?label=Blotting%20Kits&pimId=3617065")
        +> Paginate("a[title=Next]", delay = 2.second)).head.self.resolve(spooky)

      val numPages = results.head.asInstanceOf[Page].children("div.right a").size

      assert(results.size == numPages)
    }

    finally {
      spooky.conf.remoteResourceTimeout = 60.seconds
    }
  }
}