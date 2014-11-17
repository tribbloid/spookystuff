package org.tribbloid.spookystuff.example.network

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * Created by peng on 9/7/14.
 */
object Whatsmyuseragent extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    //    spooky.driverFactory = TorDriverFactory()

    noInput
      .fetch(
        Visit("http://www.whatsmyuseragent.com/")
      )
      .extract("ip" -> (_.text1("h3.info")))
      .extract("user-agent" -> (_.text1("h2.info")))
      .extract("referer" -> (_.text1("table.table-striped")))
      .asSchemaRDD()
  }
}