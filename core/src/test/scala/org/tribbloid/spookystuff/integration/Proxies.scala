package org.tribbloid.spookystuff.integration

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity.client._

/**
* Created by peng on 9/7/14.
*/
object Proxies extends ProxyFeed {

  override def doMain() = proxyRDD
}