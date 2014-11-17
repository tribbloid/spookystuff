package org.tribbloid.spookystuff.example.network

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * Created by peng on 9/7/14.
 */
object Whatismyip extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

//    spooky.driverFactory = TorDriverFactory()

    noInput
      .fetch(
        Visit("http://www.whatsmyip.org/")
      )
      .extract("ip" -> (_.text1("h1")))
      .asSchemaRDD()
  }
}
