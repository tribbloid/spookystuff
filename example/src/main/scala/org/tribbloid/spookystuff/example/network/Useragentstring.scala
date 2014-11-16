package org.tribbloid.spookystuff.example.network

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.TestCore

/**
 * Created by peng on 9/7/14.
 */
object Useragentstring extends TestCore {

  import spooky._

  override def doMain() = {

//    spooky.driverFactory = TorDriverFactory()

    noInput
      .fetch(
        Wget("http://www.useragentstring.com/pages/Browserlist/")
      )
      .extract("agent-string" -> (_.text("li a")))
      .asSchemaRDD()
  }
}
