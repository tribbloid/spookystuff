package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions.{Visit, Wget}
import com.tribbloids.spookystuff.conf.{Web, WebDriverFactory}
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import org.apache.spark.SparkException

import java.util.Date

/**
  * Created by peng on 24/11/16.
  */
class WebDriverSuite extends SpookyEnvFixture with LocalPathDocsFixture {

  import com.tribbloids.spookystuff.dsl._

  it("PhantomJS DriverFactory can degrade gracefully if remote URI is unreachable") {

    val dummyPhantomJS = WebDriverFactory.PhantomJS(
      _ =>
        WebDriverFactory.PhantomJSDeployment(
          "dummy/file",
          "dummy.org/file"
      )
    )

    try {

      reloadSpooky
      spooky.getConf(Web).webDriverFactory = dummyPhantomJS
      spookyConf.IgnoreCachedDocsBefore = Some(new Date())

      spooky
        .create(1 to 2)
        .fetch(
          Wget(HTML_URL) // deploy will fail, but PhantomJS won't be used
        )
        .count()

      intercept[SparkException] {
        val docs = spooky
          .create(1 to 2)
          .fetch(
            Visit(HTML_URL)
          )
          .docRDD

        docs.collect().foreach(println)
      }
    } finally {
      reloadSpooky
      spooky.getConf(Web).webDriverFactory = WebDriverFactory.PhantomJS()
    }
  }
}
