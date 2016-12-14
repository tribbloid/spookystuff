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

    )
  }

//  override def parallelism: Int = 1

  test("Scan 1 track per drone") {

    val tracks: Seq[(LocationLocal, LocationLocal)] = MoveSuite.generateTracks(
      parallelism,
      LocationLocal(10, 10, -10),
      LocationLocal(100, 0, 0),
      LocationLocal(0, 10, -2)
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
          assert(seq.size == 1)
          seq
      }
      .foreach(println)

    val result = spooky.create(df)
      .fetch (
        Move('_1, '_2)
      )
      .collect()
  }

//  test("Scan 2.5 tracks per drone") {
//    val tracks: Seq[(LocationLocal, LocationLocal)] = MoveSuite.generateTracks(
//      (parallelism.toDouble * 2.5).toInt,
//      LocationLocal(10, 10, 20),
//      LocationLocal(50, 50, 0),
//      LocationLocal(10, 0, 10)
//    )
//
//    val df = sql.createDataFrame(tracks)
//    df.mapPartitions {
//      itr =>
//        val seq = itr.toList
//        Iterator(seq)
//    }
//      .collect()
//      .map{
//        seq =>
//          val size = seq.size
//          assert(size >= 2)
//          assert(size <= 3)
//          seq
//      }
//      .foreach(println)
//
//    val result = spooky.create(df)
//      .fetch (
//        Move('_1, '_2)
//      )
//      .collect()
//  }
}
