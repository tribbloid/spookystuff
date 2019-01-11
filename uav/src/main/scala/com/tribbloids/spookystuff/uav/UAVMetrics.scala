package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.metrics.Metrics.Acc
import com.tribbloids.spookystuff.conf.Submodules
import com.tribbloids.spookystuff.metrics.Metrics
import org.apache.spark.util.LongAccumulator

/**
  * Created by peng on 6/22/17.
  */
case class UAVMetrics(
    //                          proxyCreated: Accumulator[Int] = SpookyMetrics.accumulator(0, "mavProxyCreated"),
    //                          proxyDestroyed: Accumulator[Int] = SpookyMetrics.accumulator(0, "mavProxyCreated"),

    linkCreated: Acc[LongAccumulator] = Metrics.accumulator(0, "linkCreated"),
    linkDestroyed: Acc[LongAccumulator] = Metrics.accumulator(0, "linkDestroyed")
    //                          linkRefitted: Accumulator[Int] = Metrics.accumulator(0, "linkRefitted")
) extends Metrics {}

object UAVMetrics extends Submodules.Builder[UAVMetrics] {

  override implicit def default = UAVMetrics()
}
