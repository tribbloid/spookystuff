package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.uav.planning.Resamplers
import com.tribbloids.spookystuff.uav.planning.TrafficControls.Avoid

class AvoidSGDRunnerSuite_InsertWP extends AvoidSGDRunnerSuite {

  override lazy val clearance = Avoid(resampler = Some(Resamplers.InsertWP()))
}
