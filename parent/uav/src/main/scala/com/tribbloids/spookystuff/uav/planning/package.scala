package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.actions.{RewriteRule, TraceView}
import com.tribbloids.spookystuff.dsl.LocalityLike
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.dsl.Localitys
import org.apache.spark.ml.uav.Vec

package object planning {

  type Constraint = RewriteRule[Vec]

  type VRPOptimizer = (Localitys.VRP, SpookySchema) => LocalityLike.Instance[TraceView]
  type Resampler = (TrafficControl, SpookySchema) => ResamplerInst
}
