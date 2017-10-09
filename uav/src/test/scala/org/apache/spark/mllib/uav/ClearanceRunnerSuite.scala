package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.NED

class ClearanceRunnerSuite extends SpookyEnvFixture {

  it("can optimize 2 very close traces") {

    val t1 = List(
      Waypoint(NED(0,0,0)),
      Waypoint(NED(1,1,0))
    )
    val t2 = List(
      Waypoint(NED(1,0,0.1)),
      Waypoint(NED(0,1,0.1))
    )
  }

  it("can optimize 2 intersecting traces") {

    val t1 = List(
      Waypoint(NED(0,0,0)),
      Waypoint(NED(1,1,0))
    )
    val t2 = List(
      Waypoint(NED(1,0,0)),
      Waypoint(NED(0,1,0))
    )
  }
}
