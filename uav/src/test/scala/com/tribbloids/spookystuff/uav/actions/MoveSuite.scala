package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.Literal
import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.sim.APMSITLFixture

object MoveSuite{

  def generateLawnMowerPattern(
                                n: Int,
                                origin: NED.V,
                                dir: NED.V, // actual directions are always alternating
                                stride: NED.V
                              ): Seq[(NED.V, NED.V)] = {

    val result = (0 until n).map {
      i =>
        val p1 = NED.create(origin.vector + (stride.vector :* i.toDouble))
        val p2 = NED.create(p1.vector + dir.vector)
        if (i % 2 == 0) {
          p1 -> p2
        }
        else {
          p2 -> p1
        }
    }
    result
  }
}

/**
  * All tests will use Proxy by default
  */
class MoveSuite extends APMSITLFixture {

  import com.tribbloids.spookystuff.dsl._

  test("Move.toJson should work") {
    val wp1: Location = LLA(0,0,0) -> GeodeticAnchor
    val wp2: Location = LLA(20, 30, 50) -> GeodeticAnchor

    val move = Move(Literal(wp1), Literal(wp2))

    move.prettyJSON().shouldBe(

    )
  }

  test("Run 1 track per drone") {

    val tracks: Seq[(NED.V, NED.V)] = MoveSuite.generateLawnMowerPattern(
      (parallelism.toDouble * 1).toInt,
      NED(10, 10, -10),
      NED(100, 0, 0),
      NED(0, 20, -2)
    )

    val rdd = sc.parallelize(tracks, this.parallelism)
    val df = sql.createDataFrame(rdd)

    val result = spooky.create(df)
      .fetch (
        Move('_1, '_2)
          +> Mark(),
        genPartitioner = GenPartitioners.Narrow // current fetchOptimizer is kaput, reverts everything to hash partitioner
      )
      .toObjectRDD(S.formattedCode)
      .collect()

    result.foreach(println)
  }

  test("Run 1.5 track per drone") {

    val tracks: Seq[(NED.V, NED.V)] = MoveSuite.generateLawnMowerPattern(
      (parallelism.toDouble * 1.5).toInt,
      NED(10, 10, -10),
      NED(100, 0, 0),
      NED(0, 20, -2)
    )

    val rdd = sc.parallelize(tracks, this.parallelism)
    val df = sql.createDataFrame(rdd)

    val result = spooky.create(df)
      .fetch (
        Move('_1, '_2)
          +> Mark(),
        genPartitioner = GenPartitioners.Narrow // current fetchOptimizer is kaput, reverts everything to hash partitioner
      )
      .collect()
  }
}