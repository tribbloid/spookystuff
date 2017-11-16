package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.uav.planning.Resamplers
import com.tribbloids.spookystuff.uav.planning.Traffics.Clearance

class ClearanceSGDRunnerSuite_WithResampler extends ClearanceSGDRunnerSuite {

  override lazy val clearance = Clearance(resampler = Some(Resamplers.InsertWaypoint()))
}
