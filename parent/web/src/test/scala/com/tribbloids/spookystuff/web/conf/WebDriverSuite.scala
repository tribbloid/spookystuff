package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.testutils.SpookyEnvSpec.defaultCtx
import com.tribbloids.spookystuff.web.actions.Visit
import org.apache.spark.SparkException

import java.util.Date

/**
  * Created by peng on 24/11/16.
  */
class WebDriverSuite extends SpookyBaseSpec with FileDocsFixture {

  it("PhantomJS DriverFactory can degrade gracefully if remote URI is unreachable") {

    val dummyPhantomJS = WebDriverFactory.PhantomJS(_ =>
      WebDriverFactory.PhantomJSDeployment(
        "dummy/file",
        "dummy.org/file"
      )
    )

    try {

      defaultCtx

      spooky(Web).confUpdate(
        _.copy(
          webDriverFactory = dummyPhantomJS
        )
      )

      spooky.confUpdate(
        _.copy(
          IgnoreCachedDocsBefore = Some(new Date())
        )
      )

      spooky
        .create(1 to 2)
        .fetch(_ =>
          Wget(HTML_URL) // deploy will fail, but PhantomJS won't be used
        )
        .count()

      intercept[SparkException] {
        val docs = spooky
          .create(1 to 2)
          .fetch(_ => Visit(HTML_URL))
          .map(_.observations)

        docs.collect().foreach(println)
      }
    } finally {
//      getDefaultCtx

//      spooky.getConf(Web).webDriverFactory = WebDriverFactory.PhantomJS()
    }
  }
}
