package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.uav.planning.Resamplers
import com.tribbloids.spookystuff.uav.planning.TrafficControls.Avoid

class AvoidSGDRunnerSuite_InsertWP extends AvoidSGDRunnerSuite {

  override lazy val clearance = Avoid(resampler = Some(Resamplers.InsertWP()))

  override def expected1: String =
    """
      |NED:POINT (0 0 0.102472)	NED:POINT (0.5 0.5 1.314936)	NED:POINT (1 1 0.102534)
      |NED:POINT (0 1 -0.099899)	NED:POINT (0.5 0.5 -1.310681)	NED:POINT (1 0 -0.099559)
    """.stripMargin

  override def expected3: String =
    """
      |NED:POINT (0 0 -0.133464)	NED:POINT (0.5 0.5 -1.222158)	NED:POINT (1 1 -0.132335)
      |NED:POINT (0 1 0.134846)	NED:POINT (0.5 0.5 1.223435)	NED:POINT (1 0 0.135634)
      |---
      |NED:POINT (0 0 0.133464)	NED:POINT (0.5 0.5 1.222158)	NED:POINT (1 1 0.132335)
      |NED:POINT (0 1 -0.134846)	NED:POINT (0.5 0.5 -1.223435)	NED:POINT (1 0 -0.135634)
    """.stripMargin

  override def expected4: String =
    """
      |NED:POINT (0 0 -0.100775)	NED:POINT (0.5 0.5 -2.520769)	NED:POINT (1 1 -0.099959)	NED:POINT (1.5 0.5 -2.524276)	NED:POINT (2 0 -0.099026)
      |NED:POINT (0 1 0.102171)	NED:POINT (0.5 0.5 2.528002)	NED:POINT (1 0 0.102521)	NED:POINT (1.5 0.5 2.528214)	NED:POINT (2 1 0.102622)
    """.stripMargin

  override def expected5: String =
    """
      |NED:POINT (0 0 -0.303549)
      |NED:POINT (0.5 0.5 -0.6281)	NED:POINT (1 1 -0.304131)
      |NED:POINT (0 1 0.307432)
      |NED:POINT (0.5 0.5 0.631511)	NED:POINT (1 0 0.307073)
    """.stripMargin
}
