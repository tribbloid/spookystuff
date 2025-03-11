//package com.tribbloids.spookystuff.uav.actions
//
//import com.tribbloids.spookystuff.uav.UAVTestUtils
//import com.tribbloids.spookystuff.uav.sim.APMQuadFixture
//import com.tribbloids.spookystuff.uav.spatial._
//import org.apache.spark.ml.dsl.utils.RecursiveMessageRelay
//
///**
//  * All tests will use Proxy by default
//  */
//class MoveIT extends APMQuadFixture {
//
//  import com.tribbloids.spookystuff.dsl._
//
//  it("toJson should work") {
//    val wp1: Location = LLA(0,0,0) -> GeodeticAnchor
//    val wp2: Location = LLA(20, 30, 50) -> GeodeticAnchor
//
//    val move = Move(wp1, wp2)
//
//    RecursiveMessageRelay.toMessage(move).prettyJSON.shouldBe(
//
//    )
//  }
//
//  val tracks: Seq[(NED.V, NED.V)] = UAVTestUtils.LawnMowerPattern(
//    (parallelism.toDouble * 1).toInt,
//    NED(10, 10, -10),
//    NED(100, 0, 0),
//    NED(0, 20, -2)
//  ).neds
//
//  it("Run 1 track per drone") {
//
//    val rdd = sc.parallelize(tracks, this.parallelism)
//    val df = sql.createDataFrame(rdd)
//
//    val result = spooky.create(df)
//      .fetch (
//        Move('_1, '_2)
//          +> Mark(),
//        locality = Localitys.Narrow // current locality is ill-suited
//      )
//      .toObjectRDD(S.formattedCode)
//      .collect()
//
//    result.foreach(println)
//  }
//
//  it("Run 1.5 track per drone") {
//
//    val rdd = sc.parallelize(tracks, this.parallelism)
//    val df = sql.createDataFrame(rdd)
//
//    val result = spooky.create(df)
//      .fetch (
//        Move('_1, '_2)
//          +> Mark(),
//        locality = Localitys.Narrow
//      )
//      .collect()
//  }
//}
