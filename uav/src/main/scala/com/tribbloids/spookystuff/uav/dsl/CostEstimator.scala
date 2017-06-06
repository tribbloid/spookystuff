package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.planning.{FromLocation, PreferLink}
import com.tribbloids.spookystuff.uav.spatial.NED

//TODO: this API may take too long to extend, should delegate most of it to UAVAction
trait CostEstimator {

  def estimate(
                traces: Seq[Trace],
                spooky: SpookyContext
              ): Double = 0
}

object CostEstimator {

  case class Default(
                      navSpeed: Double
                    ) extends CostEstimator {

    def intraCost(nav: UAVNavigation) = {

      val ned = nav._to.getCoordinate(NED, nav._from).get
      val distance = Math.sqrt(ned.vector dot ned.vector)

      val speed = nav.speedOpt.getOrElse(navSpeed)

      distance / speed
    }

    def interCost(nav1: UAVNavigation, nav2: UAVNavigation) = {

      val end1 = nav1._to
      val start2 = nav2._from

      val ned = start2.getCoordinate(NED, end1).get
      val distance = Math.sqrt(ned.vector dot ned.vector)

      distance / navSpeed
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

      val firstLocation = useLink.head.firstLink.status().currentLocation

      var prev: UAVNavigation = FromLocation(firstLocation)
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
