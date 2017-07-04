package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.Metrics
import com.tribbloids.spookystuff.conf.Submodules
import org.apache.spark.Accumulator

/**
  * Created by peng on 6/22/17.
  */
case class UAVMetrics(
                       //                          proxyCreated: Accumulator[Int] = SpookyMetrics.accumulator(0, "mavProxyCreated"),
                       //                          proxyDestroyed: Accumulator[Int] = SpookyMetrics.accumulator(0, "mavProxyCreated"),

                       linkCreated: Accumulator[Int] = Metrics.accumulator(0, "linkCreated"),
                       linkDestroyed: Accumulator[Int] = Metrics.accumulator(0, "linkDestroyed")
                       //                          linkRefitted: Accumulator[Int] = Metrics.accumulator(0, "linkRefitted")
                     ) extends Metrics {

}

object UAVMetrics extends Submodules.Builder[UAVMetrics] {

  override implicit def default = UAVMetrics()
}