package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.planning.PreferUAV
import com.tribbloids.spookystuff.uav.spatial.NED

//TODO: this API may take too long to extend, should delegate most of it to UAVAction
trait CostEstimator {

  def estimate(
                trace: Trace,
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
                           trace: Trace,
                           spooky: SpookyContext
                         ): Double = {

      val concated: Seq[Action] = trace

      {
        val preferUAVs = concated.collect {
          case v: PreferUAV => v
        }
        require(preferUAVs.size <= 1,
          s"attempt to dispatch ${preferUAVs.size} UAVs for a task," +
            " only 1 UAV can be dispatched for a task." +
            " (This behaviour is likely permanent and won't be fixed in the future)"
        )
      }

      val navs: Seq[UAVNavigation] = concated.collect {
        case nav: UAVNavigation => nav
      }

      val costSum = navs.indices.map {
        i =>
          val c1 = intraCost(navs(i))
          val c2 = if (i >= navs.size - 1) 0
          else interCost(navs(i), navs(i + 1))
          c1 + c2
      }
        .sum

      costSum
    }
  }
}
