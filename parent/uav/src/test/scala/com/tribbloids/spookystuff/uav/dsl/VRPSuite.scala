package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.row.DataRow
import com.tribbloids.spookystuff.uav.actions.{Takeoff, Waypoint}
import com.tribbloids.spookystuff.uav.planning.PreferUAV
import com.tribbloids.spookystuff.uav.planning.VRPOptimizers.VRPFixture
import com.tribbloids.spookystuff.uav.spatial.point.NED
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.LinkUtils
import com.tribbloids.spookystuff.uav.{DummyUAVFixture, UAVConf, UAVTestUtils}

/**
  * Created by peng on 16/06/17.
  */
class VRPSuite extends DummyUAVFixture with VRPFixture {

  override def parallelism: Int = 4 //TODO: this makes test unable to be run on smaller machine and waste resources

  def pattern(n: Int) = UAVTestUtils.LawnMowerPattern(
    n,
    NED(10, 10, -10),
    NED(100, 0, 0),
    NED(0, 20, -2)
  )

  def waypoints(n: Int): Seq[List[Waypoint]] =
    pattern(n).waypoints
      .map { v =>
        List(v)
      }

  def lineScans(n: Int): Seq[List[Waypoint]] = pattern(n).lineScans

  def runTest(
      traces: Seq[Trace]
  ): Array[List[TraceView]] = {

    LinkUtils.unlockAll(sc)

    val rdd = sc
      .parallelize(
        traces
      )
      .map { trace =>
        val k = TraceView(trace)
        k -> DataRow()
      }

    spooky.rebroadcast()
    val gp = getVRP
    val gpInst = gp.Inst(defaultSchema)

    val groupedRDD = gpInst.groupByKey(rdd)

    val grouped = groupedRDD.keys
      .mapPartitions { itr =>
        Iterator(itr.toList)
      }
      .collect()

    defaultSchema.ec.scratchRDDs.clearAll()

    LinkUtils.unlockAll(sc)

    grouped
  }

  def getCost(grouped: Array[List[TraceView]]) = {

    val uav_lengths: Array[(UAV, Double)] = grouped.flatMap { path =>
      val actions = path.flatMap(_.children)
      if (actions.isEmpty) None
      else {
        val statusSeq = actions.collect {
          case PreferUAV(uav, _) => uav
        }.distinct
        assert(statusSeq.size == 1)
        val status = statusSeq.head
        val first = Waypoint(status.currentLocation)
        val others = actions.flatMap {
          case PreferUAV(uav, _) => None
          case v @ _             => Some(v)
        }

        val _trace = List(first) ++ others
        val cost = spooky.getConf[UAVConf].costEstimator.estimate(_trace, defaultSchema)
        Some(status.uav -> cost)
      }
    }

    uav_lengths.foreach(v => println(s"Length = ${v._2}m for ${v._1}"))

    val lengths: Array[Double] = uav_lengths.map(_._2)
    val cost = lengths.max

    println(s"Max length = ${cost}m")
    cost
  }

  it("can optimize max cost of 2 waypoints per UAV") {

    val grouped = runTest(waypoints(parallelism))
    assert(getCost(grouped) <= 142)
  }

  it("can optimize max cost of 5 waypoints per UAV") {

    val grouped = runTest(waypoints((parallelism * 2.5).toInt))
    assert(getCost(grouped) <= 228)
  }

  it("can optimize max cost of 2 line scan per UAV") {

    val grouped = runTest(lineScans(parallelism * 2))
    assert(getCost(grouped) <= 353)
  }

  it("can optimize max cost of 5 line scans per UAV") {

    val grouped = runTest(lineScans(parallelism * 5))
    assert(getCost(grouped) <= 833)
  }

  it("can optimize max cost of 2 takeoff + line scan per UAV") {

    val scans = lineScans(parallelism * 2)
    val withTakeoff = scans.map { scan =>
      List(Takeoff(10, 100)) ++ scan
    }

    val grouped = runTest(withTakeoff)
    assert(getCost(grouped) <= 362)
  }
}
