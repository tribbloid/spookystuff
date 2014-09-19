package org.tribbloid.spookystuff.integration

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.factory.driver.TorDriverFactory

/**
 * Created by peng on 9/7/14.
 */
object Whatismyip extends SpookyTestCore {

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
