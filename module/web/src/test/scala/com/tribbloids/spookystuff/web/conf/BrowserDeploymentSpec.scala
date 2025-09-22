package com.tribbloids.spookystuff.web.conf

class BrowserDeploymentSpec {

  //  describe("PhantomJS deployment") {
  //
  //    it("can deploy normally") {
  //
  //      val deployment: WebDriverFactory.BrowserDeployment = WebDriverFactory.BrowserDeployment()
  //
  //      val path = deployment.deploydPath
  //
  //      assert(path.endsWith("phantomjs") || path.endsWith("phantomjs.exe"))
  //    }
  //
  //    it("can degrade gracefully if remote URI is unreachable") {
  //
  //      val dummyPhantomJS = WebDriverFactory.PhantomJS(_ =>
  //        WebDriverFactory.BrowserDeployment(
  //          "dummy/file",
  //          "dummy.org/file"
  //        )
  //      )
  //
  //      try {
  //
  //        val spooky = defaultCtx.copy()
  //
  //        spooky(Web).confUpdate(
  //          _.copy(
  //            webDriverFactory = dummyPhantomJS
  //          )
  //        )
  //
  //        spooky.confUpdate(
  //          _.copy(
  //            IgnoreCachedDocsBefore = Some(new Date())
  //          )
  //        )
  //
  //        Wget(HTML_URL)
  //          .as("T")
  //          .fetch(spooky) // deploy will fail, but PhantomJS won't be used
  //
  //        intercept[Exception] {
  //
  //          (
  //            Visit(HTML_URL) +>
  //              Snapshot().as("T")
  //          ).fetch(spooky)
  //        }
  //      } finally {}
  //    }
  //  }
}
