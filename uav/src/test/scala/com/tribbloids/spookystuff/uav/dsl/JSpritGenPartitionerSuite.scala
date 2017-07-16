package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.actions.{Action, TraceView}
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.DataRow
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.planning.{JSpritFixture, PreferUAV, WaypointPlaceholder}
import com.tribbloids.spookystuff.uav.spatial.NED
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.{UAVConf, UAVFixture, UAVTestUtils}
import org.scalatest.Ignore

/**
  * Created by peng on 16/06/17.
  */
@Ignore //TODO: problem should be set up to be invariant to number of cores
class JSpritGenPartitionerSuite extends UAVFixture with JSpritFixture {

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
    val gp = getJSprit
    val inst = gp.Inst(ec)

    val groupedRDD = inst.groupByKey(rdd)

    val grouped = groupedRDD.keys.mapPartitions {
      itr =>
        Iterator(itr.toList)
    }
      .collect()
    grouped
  }

  def getCost(grouped: Array[List[TraceView]]) = {
    val uav_lengths: Array[(UAV, Double)] = grouped.flatMap {
      path =>
        val actions = path.flatMap(_.children)
        if (actions.isEmpty) None
        else {
          val statusSeq = actions.collect {
            case PreferUAV(uav) => uav.status()
          }
            .distinct
          assert(statusSeq.size == 1)
          val status = statusSeq.head
          val first = WaypointPlaceholder(status.currentLocation)
          val others = actions.flatMap {
            case PreferUAV(uav) => None
            case v@_ => Some(v)
          }

          val cost = spooky.getConf[UAVConf].costEstimator.estimate(List(first) ++ others, spooky)
          Some(status.uav -> cost)
        }
    }

    uav_lengths.foreach(v => println(s"Length = ${v._2}m for ${v._1}"))

    val lengths: Array[Double] = uav_lengths.map(_._2)
    val cost = lengths.max

    println(s"Max length = ${cost}m")
    cost
  }

  it("can optimize max cost of 1 waypoint per UAV") {

    val grouped = runTest(waypoints(parallelism))
    assert(getCost(grouped) <= 141.739)
  }

  it("can optimize max cost of 2.5 waypoints per UAV") {

    val grouped = runTest(waypoints((parallelism * 2.5).toInt))
    assert(getCost(grouped) <= 231.504)
  }

  it("can optimize max cost of 1 scan per UAV") {


  }
}
