package org.tribbloid.spookystuff.integration

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.factory.driver._

/**
 * Created by peng on 9/7/14.
 */
object Useragentstring extends SpookyTestCore {

  import spooky._

  override def doMain() = {

    spooky.driverFactory = TorDriverFactory()

    (noInput
      +> Wget("http://www.useragentstring.com/pages/Browserlist/")
      !=!())
      .extract("agent-string" -> (_.text("li a")))
      .asSchemaRDD()
  }
}
