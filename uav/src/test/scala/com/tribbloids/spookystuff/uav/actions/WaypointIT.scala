package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.uav.UAVTestUtils
import com.tribbloids.spookystuff.uav.sim.APMQuadFixture
import com.tribbloids.spookystuff.uav.spatial._
import org.apache.spark.ml.dsl.utils.RecursiveMessageRelay

/**
  * Created by peng on 16/07/17.
  */
class WaypointIT extends APMQuadFixture {

  import com.tribbloids.spookystuff.dsl._

  it("toJson should work") {
    val wp1: Location = LLA(0,0,0) -> GeodeticAnchor

    val move = Waypoint(wp1)

    RecursiveMessageRelay.toMessage(move).prettyJSON.shouldBe(

    )
  }

  val pattern: Seq[(NED.V, NED.V)] = UAVTestUtils.LawnMowerPattern(
    (parallelism.toDouble * 1).toInt,
    NED(10, 10, -10),
    NED(100, 0, 0),
    NED(0, 20, -2)
  )
    .neds

  it("Run 1 track per drone") {

    val rdd = sc.parallelize(pattern, this.parallelism)
    val df = sql.createDataFrame(rdd)

    val result = spooky.create(df)
      .fetch (
        Waypoint('_1) +> Waypoint('_2) +> Mark(),
        genPartitioner = GenPartitioners.Narrow // current genPartitioner is ill-suited
      )
      .toObjectRDD(S.formattedCode)
      .collect()

    result.foreach(println)
  }

  it("Run 1.5 track per drone") {

    val rdd = sc.parallelize(pattern, this.parallelism)
    val df = sql.createDataFrame(rdd)

    val result = spooky.create(df)
      .fetch (
        Waypoint('_1) +> Waypoint('_2) +> Mark(),
        genPartitioner = GenPartitioners.Narrow
      )
      .collect()
  }
}
