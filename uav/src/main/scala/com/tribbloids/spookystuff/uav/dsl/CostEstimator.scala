package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.planning.PreferLink
import com.tribbloids.spookystuff.uav.spatial.{NED, StartEndLocation}

//TODO: this API may take too long to extend, should delegate most of it to UAVAction
trait CostEstimator {

  def estimate(
                traces: Seq[Trace],
                spooky: SpookyContext
              ): Double = 0
}

object CostEstimator {

  case class Default(
                      defaultNavSpeed: Double
                    ) extends CostEstimator {

    def intraCost(nav: StartEndLocation) = {
      import nav._

      val ned = end.getCoordinate(NED, start).get
      val distance = Math.sqrt(ned.vector dot ned.vector)

      distance / speed
    }

    def interCost(nav1: StartEndLocation, nav2: StartEndLocation) = {

      val end1 = nav1.end
      val start2 = nav2.start

      val ned = start2.getCoordinate(NED, end1).get
      val distance = Math.sqrt(ned.vector dot ned.vector)

      distance / defaultNavSpeed
    }

    override def estimate(
                           traces: Seq[Trace],
                           spooky: SpookyContext
                         ): Double = {

      val concated: Seq[Action] = traces.flatten
      val navs: Seq[UAVNavigation] = concated.collect {
        case nav: UAVNavigation => nav
      }
      val useLink = concated.collect {
        case v: PreferLink => v
      }
        .distinct

      require(useLink.size == 1,
        s"attempt to dispatch ${useLink.size} drone for a task," +
          " only 1 drone can be dispatched for a task." +
          " (This behaviour is likely permanent and won't be fixed in the future)"
      )

      val firstLocation = useLink.head.firstLink.currentLocation()

      var prev: StartEndLocation = firstLocation
      var costSum = 0.0
      navs.foreach {
        nav =>
          val c1 = intraCost(nav)
          val c2 = interCost(prev, nav)
          prev = nav
          costSum += c1 + c2
      }

      costSum
    }
  }
}
