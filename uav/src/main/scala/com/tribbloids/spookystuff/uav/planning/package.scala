package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.actions.{RewriteRule, TraceView}
import com.tribbloids.spookystuff.dsl.GenPartitionerLike
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import org.apache.spark.ml.uav.Vec

package object planning {

  type Constraint = RewriteRule[Vec]

  type VRPOptimizer = (GenPartitioners.VRP, SpookySchema) => GenPartitionerLike.Instance[TraceView]
  type Resampler = (TrafficControl, SpookySchema) => ResamplerInst
}
