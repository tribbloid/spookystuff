package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.uav.actions.{UAVNavigation, UseFleet}
import com.tribbloids.spookystuff.uav.spatial.{NED, StartEndLocation}

trait ActionCosts {

  def estimate(
                vss: Iterator[Trace],
                spooky: SpookyContext
              ): Double = 0
}

object ActionCosts {

  case class Default(
                      defaultNavSpeed: Double
                    ) extends ActionCosts {

    def cost1(nav: StartEndLocation) = {
      import nav._

      val ned = end.getCoordinate(NED, start).get
      val distance = Math.sqrt(ned.vector dot ned.vector)

      distance / speed
    }

    def cost2(nav1: StartEndLocation, nav2: StartEndLocation) = {

      val end1 = nav1.end
      val start2 = nav2.start

      val ned = start2.getCoordinate(NED, end1).get
      val distance = Math.sqrt(ned.vector dot ned.vector)

      distance / defaultNavSpeed
    }

    override def estimate(
                           vss: Iterator[Trace],
                           spooky: SpookyContext
                         ): Double = {

      val concated: Iterator[Action] = vss.flatten
      val navs: Iterator[UAVNavigation] = concated.collect {
        case nav: UAVNavigation => nav
      }
      val useFleets = concated.collect {
        case v: UseFleet => v
      }.toSeq.distinct

      require(useFleets.size == 1,
        s"attempt to dispatch ${useFleets.size} drone for a task," +
          " only 1 drone can be dispatched for a task." +
          " (This behaviour is likely permanent and won't be fixed in the future)"
      )

      val executedBy = useFleets.head.drones
      assert(executedBy.size == 1)

      val link = executedBy.head.toLink(spooky, tryConnect = true)
      val firstLocation = link.getLocation(true)

      var prev: StartEndLocation = firstLocation
      var costsum = 0.0
      navs.foreach {
        nav =>
          val c1 = cost1(nav)
          val c2 = cost2(prev, nav)
          prev = nav
          costsum += c1 + c2
      }

      costsum
    }
  }
}
