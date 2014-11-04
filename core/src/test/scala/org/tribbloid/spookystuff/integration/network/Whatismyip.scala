package org.tribbloid.spookystuff.integration.network

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.factory.driver.TorDriverFactory
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 9/7/14.
 */
object Whatismyip extends TestCore {

  import spooky._

  override def doMain() = {

//    spooky.driverFactory = TorDriverFactory()

    (noInput
      +> Visit("http://www.whatsmyip.org/")
      !=!())
      .extract("ip" -> (_.text1("h1")))
      .asSchemaRDD()
  }
}
