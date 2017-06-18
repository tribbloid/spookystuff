//package com.tribbloids.spookystuff.uav.planning
//
//import com.tribbloids.spookystuff.SpookyEnvFixture
//
///**
//  * Created by peng on 11/06/17.
//  */
//class RDDSpike extends SpookyEnvFixture {
//
//  describe("RDD.cogroup") {
//    it("can preserve order of data inside partition") {
//
//      val rdd1 = sc.parallelize(1 to 100).keyBy(v => v)
//
//      val rdd2 = sc.parallelize(200.to(1, -1)).keyBy(v => v)
//
//      val cogrouped = rdd1.cogroup(rdd2).persist()
//      cogrouped.collect().foreach(println)
//
//      val seq = cogrouped.flatMap {
//        tuple =>
//          Predef.assert(tuple._2._2.size == 1)
//          val q2 = tuple._2._2.head
//
//          Predef.assert(tuple._2._1.size <= 1)
//          val q1Opt = tuple._2._1.headOption
//
//          q1Opt.foreach {
//            q1 =>
//              Predef.assert(q1 == q2)
//          }
//          q1Opt
//      }
//        .collect().toSeq
//
//      assert(seq == (1 to 100))
//    }
//  }
//}
