package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.extractors.Literal
import com.tribbloids.spookystuff.mav.sim.APMSimFixture

object MoveSuite{

  def generateTracks(
                      n: Int,
                      origin: LocationLocal,
                      dir: LocationLocal, // actual directions are always alternating
                      stride: LocationLocal
                    ): Seq[(LocationLocal, LocationLocal)] = {

    val result = (0 until n).map {
      i =>
        val p1: LocationLocal = origin + (stride :* i.toDouble)
        val p2: LocationLocal = p1 + dir
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
class MoveSuite extends APMSimFixture {

  import com.tribbloids.spookystuff.dsl._

  test("Move.toJson should work") {
    val wp1 = LocationGlobal(0,0,0)
    val wp2 = LocationGlobal(20, 30, 50)

    val move = Move(Literal(wp1), Literal(wp2))

    move.toMessage.prettyJSON().shouldBe(
      """{
        |  "className" : "com.tribbloids.spookystuff.mav.actions.Move",
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

  //  override def parallelism: Int = 1

  //TODO: add Mark result validations!
  test("Run 1.5 track per drone") {

    val tracks: Seq[(LocationLocal, LocationLocal)] = MoveSuite.generateTracks(
      (parallelism.toDouble * 1.5).toInt,
      LocationLocal(10, 10, -10),
      LocationLocal(100, 0, 0),
      LocationLocal(0, 20, -2)
    )

    val rdd = sc.parallelize(tracks, this.parallelism)
    val df = sql.createDataFrame(rdd)

    df.collect()

//    df.mapPartitions {
//      itr =>
//        val seq = itr.toList
//        Iterator(seq)
//    }
//      .collect()
//      .map{
//        seq =>
//          assert(seq.size == 1)
//          seq
//      }
//      .foreach(println)

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

  //TODO: TOOOO slow and blocked by https://github.com/dronekit/dronekit-python/issues/688
  test("Run 2.5 track per drone") {

    val tracks: Seq[(LocationLocal, LocationLocal)] = MoveSuite.generateTracks(
      (parallelism.toDouble * 2.5).toInt,
      LocationLocal(10, 10, -10),
      LocationLocal(100, 0, 0),
      LocationLocal(0, 20, -2)
    )

    val rdd = sc.parallelize(tracks, this.parallelism)
    val df = sql.createDataFrame(rdd)

    df.collect()

    df.mapPartitions {
      itr =>
        val seq = itr.toList
        Iterator(seq)
    }
      .collect()
      .map{
        seq =>
          val size = seq.size
          assert(size >= 2)
          assert(size <= 3)
          seq
      }
      .foreach(println)

    val result = spooky.create(df)
      .fetch (
        Move('_1, '_2)
          +> Mark(),
        genPartitioner = GenPartitioners.Narrow // current fetchOptimizer is kaput, reverts everything to hash partitioner
      )
      .collect()
  }
}