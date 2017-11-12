package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.actions.{RewriteRule, Trace, TraceView}
import com.tribbloids.spookystuff.dsl.GenPartitionerLike
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import org.apache.spark.mllib.uav.Vec

package object planning {

  type Resampler = RewriteRule[Trace]
  type Constraint = RewriteRule[Vec]

  type VRPOptimizer = (GenPartitioners.VRP, DataRowSchema) => GenPartitionerLike.Instance[TraceView]
}
