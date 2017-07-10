package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.actions.{Action, TraceView}
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.DataRow
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.planning.{PreferUAV, WrapLocation}
import com.tribbloids.spookystuff.uav.spatial.NED
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.{UAVConf, UAVFixture, UAVTestUtils}

/**
  * Created by peng on 16/06/17.
  */
//@Ignore
class JSpritGenPartitionerSuite extends UAVFixture {

  override def simURIs = (0 until parallelism).map {
    v =>
      s"dummy:localhost:$v"
  }

  def waypoints(n:Int): Seq[Waypoint] = UAVTestUtils.LawnMowerPattern(
    n,
    NED(10, 10, -10),
    NED(100, 0, 0),
    NED(0, 20, -2)
  )
    .waypoints

  lazy val genPartitioner = GenPartitioners.JSprit()

  def runTest(
               wps: Seq[Action]
             ): Array[List[TraceView]] = {

    val rdd = sc.parallelize(
      wps
    ).map {
      wp =>
        val k = TraceView(List(wp))
        k -> DataRow()
    }

    spooky.rebroadcast()
    val ec = ExecutionContext(spooky)
    val inst = genPartitioner.Inst(ec)

    val groupedRDD = inst.groupByKey(rdd)

    val grouped = groupedRDD.keys.mapPartitions{
      itr =>
        Iterator(itr.toList)
    }
      .collect()
    grouped.foreach(println)
    grouped
  }

  def getCost(grouped: Array[List[TraceView]]) = {
    val uav_lengths: Array[(UAV, Double)] = grouped.map {
      path =>
        val actions = path.flatMap(_.children)
        if (actions.isEmpty) UAV(Seq("non-existance")) -> 0.0
        else {
          val statusSeq = actions.collect {
            case PreferUAV(uav) => uav
          }
            .distinct
          assert(statusSeq.size == 1)
          val status = statusSeq.head
          val first = WrapLocation(status.currentLocation)
          val others = actions.flatMap {
            case PreferUAV(uav) => None
            case v@_ => Some(v)
          }

          val cost = spooky.getConf[UAVConf].costEstimator.estimate(List(first) ++ others, spooky)
          status.uav -> cost
        }
    }

    uav_lengths.foreach(v => println(s"UAV ${v._1} will move ${v._2}m"))

    val lengths: Array[Double] = uav_lengths.map(_._2)
    val cost = lengths.max

    println(s"total cost = $cost")
    cost
  }


  it("can optimize max cost of 1 waypoint per UAV") {

    val grouped = runTest(waypoints(parallelism))
    assert(getCost(grouped) <= 191.662)
  }

  it("can optimize max cost of 2.5 waypoints per UAV") {

    val grouped = runTest(waypoints((parallelism * 2.5).toInt))
    assert(getCost(grouped) <= 291.804)
  }

  it("can optimize max cost of 1 scan per UAV") {


  }
}
