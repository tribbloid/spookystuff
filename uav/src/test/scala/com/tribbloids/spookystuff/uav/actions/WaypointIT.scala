package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.uav.UAVTestUtils
import com.tribbloids.spookystuff.uav.sim.APMQuadFixture
import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.spatial.point.{LLA, Location, NED}

/**
  * Created by peng on 16/07/17.
  */
class WaypointIT extends APMQuadFixture {

  import com.tribbloids.spookystuff.dsl._

  it("toJson should work") {
    val wp1: Location = LLA(0, 0, 0) -> Anchors.Geodetic

    val move = Waypoint(wp1)

    move.prettyJSON.shouldBe(
      // TODO: missing
    )
  }

  val pattern = UAVTestUtils
    .LawnMowerPattern(
      (parallelism.toDouble * 1).toInt,
      NED(10, 10, -10),
      NED(100, 0, 0),
      NED(0, 20, -2)
    )
    .coordinates

  val rdd = sc.parallelize(pattern, this.parallelism)
  val df = sql.createDataFrame(rdd)

  it("Run 1 track per drone") {

    val result = spooky
      .create(df)
      .fetch(
        Waypoint('_1) +> Waypoint('_2) +> Mark(),
        genPartitioner = GenPartitioners.Narrow // current genPartitioner is ill-suited
      )
      .toObjectRDD(S.formattedCode)
      .collect()

    result.foreach(println) // TODO: validate location
  }

  it("Run 1.5 track per drone") {

    val result = spooky
      .create(df)
      .fetch(
        Waypoint('_1) +> Waypoint('_2) +> Mark(),
        genPartitioner = GenPartitioners.Narrow
      )
      .collect()

    result.foreach(println)
  }
}
