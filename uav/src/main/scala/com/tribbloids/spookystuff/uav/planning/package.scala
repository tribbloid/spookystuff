package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.actions.{RewriteRule, Trace}
import org.apache.spark.mllib.uav.Vec

package object planning {

  type Resampler = RewriteRule[Trace]
  type Constraint = RewriteRule[Vec]
}
