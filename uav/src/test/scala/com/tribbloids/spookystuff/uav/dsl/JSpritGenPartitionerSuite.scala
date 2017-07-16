package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.DataRow
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.planning.{JSpritFixture, PreferUAV, WaypointPlaceholder}
import com.tribbloids.spookystuff.uav.spatial.NED
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.{DummyUAVFixture, UAVConf, UAVTestUtils}

/**
  * Created by peng on 16/06/17.
  */
class JSpritGenPartitionerSuite extends DummyUAVFixture with JSpritFixture {

  override def parallelism: Int = 8

  def pattern(n: Int) = UAVTestUtils.LawnMowerPattern(
    n,
    NED(10, 10, -10),
    NED(100, 0, 0),
    NED(0, 20, -2)
  )

  def waypoints(n: Int): Seq[List[Waypoint]] = pattern(n)
    .waypoints
    .map {v => List(v)}

  def lineScans(n: Int): Seq[List[Waypoint]] = pattern(n)
    .lineScans

  def runTest(
               traces: Seq[Trace]
             ): Array[List[TraceView]] = {

    val rdd = sc.parallelize(
      traces
    ).map {
      trace =>
        val k = TraceView(trace)
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
            case PreferUAV(uav) => uav
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

  it("can optimize max cost of 2 waypoint per UAV") {

    val grouped = runTest(waypoints(parallelism))
    assert(getCost(grouped) <= 187.553)
  }

  it("can optimize max cost of 3.5 waypoints per UAV") {

    val grouped = runTest(waypoints((parallelism * 1.75).toInt))
    assert(getCost(grouped) <= 293.762)
  }

  it("can optimize max cost of 1 line scan per UAV") {

    val grouped = runTest(lineScans(parallelism))
    assert(getCost(grouped) <= 352.327)
  }

  it("can optimize max cost of 2.5 line scans per UAV") {

    val grouped = runTest(lineScans((parallelism * 2.5).toInt))
    assert(getCost(grouped) <= 832.726)
  }
}
