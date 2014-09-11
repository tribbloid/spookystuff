package org.tribbloid.spookystuff.integration

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.clientaction._
import org.tribbloid.spookystuff.factory.driver._

/**
 * Created by peng on 9/7/14.
 */
object Whatsmyuseragent extends SpookyTestCore {

  import spooky._

  override def doMain() = {

    spooky.driverFactory = TorDriverFactory()

    (noInput
      +> Visit("http://www.whatsmyuseragent.com/")
      !=!())
      .extract("ip" -> (_.text1("h3.info")))
      .extract("user-agent" -> (_.text1("h2.info")))
      .extract("referer" -> (_.text1("table.table-striped")))
      .asSchemaRDD()
  }
}
