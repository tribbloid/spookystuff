package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.row.DataRowSchema
import org.apache.spark.mllib.uav.{DVec, Vec}

object Constraints {
  object AltitudeOnly extends Constraint {
    override def rewrite(v: Vec, schema: DataRowSchema): Vec = {
      val alt = v(2)
      new DVec(Array(0,0,alt))
    }
  }
}

