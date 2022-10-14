package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.row.SpookySchema
import org.apache.spark.ml.uav.{DVec, Vec}

object Constraints {
  object AltitudeOnly extends Constraint {
    override def rewrite(v: Vec, schema: SpookySchema): Vec = {
      val alt = v(2)
      new DVec(Array(0, 0, alt))
    }
  }
}
