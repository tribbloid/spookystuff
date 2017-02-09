package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.Literal
import com.tribbloids.spookystuff.uav.spatial.{LocationGlobal, LocationLocal}
import com.tribbloids.spookystuff.uav.sim.APMSITLFixture

object MoveSuite{

  def generateTracks(
                      n: Int,
                      origin: LocationLocal,
                      dir: LocationLocal, // actual directions are always alternating
                      stride: LocationLocal
                    ): Seq[(LocationLocal, LocationLocal)] = {

    val result = (0 until n).map {
      i =>
        val p1: LocationLocal = origin.vec + (stride.vec :* i.toDouble)
        val p2: LocationLocal = p1.vec + dir.vec
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
    val wp1 = LocationGlobal(0,0,0)
    val wp2 = LocationGlobal(20, 30, 50)

    val move = Move(Literal(wp1), Literal(wp2))

    move.prettyJSON().shouldBe(
      """{
        |  "className" : "com.tribbloids.spookystuff.uav.actions.Move",
        |  "params" : {
        |    "from" : {
        |      "lat" : 0.0,
        |      "lon" : 0.0,
        |      "alt" : 0.0
        |    },
        |    "to" : {
        |      "lat" : 20.0,
        |      "lon" : 30.0,
        |      "alt" : 50.0
        |    },
        |    "delay" : null
        |  }
        |}
      """.stripMargin
    )
  }

  test("Run 1 track per drone") {

    val tracks: Seq[(LocationLocal, LocationLocal)] = MoveSuite.generateTracks(
      (parallelism.toDouble * 1).toInt,
      LocationLocal(10, 10, -10),
      LocationLocal(100, 0, 0),
      LocationLocal(0, 20, -2)
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

    val tracks: Seq[(LocationLocal, LocationLocal)] = MoveSuite.generateTracks(
      (parallelism.toDouble * 1.5).toInt,
      LocationLocal(10, 10, -10),
      LocationLocal(100, 0, 0),
      LocationLocal(0, 20, -2)
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