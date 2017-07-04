package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.planning.PreferUAV
import com.tribbloids.spookystuff.uav.spatial.NED

trait CostEstimator {

  def estimate(
                trace: Trace,
                spooky: SpookyContext
              ): Double = 0
}

object CostEstimator {

  case class Default(
                      speed: Double = 1.0
                    ) extends CostEstimator {

    def intraCost(nav: UAVNavigation) = {

      val ned = nav._to.coordinate(NED, nav._from)
      val distance = Math.sqrt(ned.vector dot ned.vector)

      val _speed = nav.speedOpt.getOrElse(speed)

      distance / _speed
    }

    def interCost(nav1: UAVNavigation, nav2: UAVNavigation) = {

      val end1 = nav1._to
      val start2 = nav2._from

      val ned = start2.coordinate(NED, end1)
      val distance = Math.sqrt(ned.vector dot ned.vector)

      distance / speed
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
