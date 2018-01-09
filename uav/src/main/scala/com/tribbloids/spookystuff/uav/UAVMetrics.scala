package com.tribbloids.spookystuff.uav

import java.lang

import com.tribbloids.spookystuff.Metrics
import com.tribbloids.spookystuff.Metrics.Acc
import com.tribbloids.spookystuff.conf.Submodules

/**
  * Created by peng on 6/22/17.
  */
case class UAVMetrics(
                       //                          proxyCreated: Accumulator[Int] = SpookyMetrics.accumulator(0, "mavProxyCreated"),
                       //                          proxyDestroyed: Accumulator[Int] = SpookyMetrics.accumulator(0, "mavProxyCreated"),

                       linkCreated: Acc[lang.Long] = Metrics.accumulator(0, "linkCreated"),
                       linkDestroyed: Acc[lang.Long] = Metrics.accumulator(0, "linkDestroyed")
                       //                          linkRefitted: Accumulator[Int] = Metrics.accumulator(0, "linkRefitted")
                     ) extends Metrics {

}

object UAVMetrics extends Submodules.Builder[UAVMetrics] {

  override implicit def default = UAVMetrics()
}
