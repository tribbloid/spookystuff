package org.tribbloid.spookystuff.integration

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.Visit
import org.tribbloid.spookystuff.factory.driver.TorDriverFactory

/**
 * Created by peng on 9/7/14.
 */
object Whatismyip extends SpookyTestCore {

  import spooky._

  override def doMain(): RDD[_] = {

    spooky.driverFactory = TorDriverFactory()

    (empty
      +> Visit("http://www.whatsmyip.org/")
      !=!())
      .select("ip" -> (_.text1("h1")))
      .asSchemaRDD()
  }
}
