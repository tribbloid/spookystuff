package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.actions.{Action, Trace, TraceView}
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.planning.{InsertPrevNavRule, PreferUAV}
import com.tribbloids.spookystuff.uav.spatial.point.NED

trait CostEstimator {

  def estimate(
                trace: Trace,
                schema: DataRowSchema
              ): Double = {

    def getTrace = {
      () =>
        InsertPrevNavRule.rewrite(trace, schema)
    }

    _estimate(getTrace, schema)
  }

  def _estimate(
                 getTrace: () => Trace,
                 schema: DataRowSchema
               ): Double = 0
}

object CostEstimator {

  case class Default(
                      speed: Double = 1.0
                    ) extends CostEstimator {

    class Instance(
                    trace: Trace,
                    schema: DataRowSchema
                  ) {

      def intraCost(nav: UAVNavigation) = {

        val ned = nav.getEnd(schema)
          .coordinate(NED, nav.getLocation(schema))
        val distance = Math.sqrt(ned.vector dot ned.vector)

        val _speed = nav.speedOpt.getOrElse(speed)

        distance / _speed
      }

      def interCost(nav1: UAVNavigation, nav2: UAVNavigation) = {

        val end1 = nav1.getEnd(schema)
        val start2 = nav2.getLocation(schema)

        val ned = start2.coordinate(NED, end1)
        val distance = Math.sqrt(ned.vector dot ned.vector)

        distance / speed
      }

      lazy val solve = {
        val spooky = schema.ec.spooky
        val concated: Seq[Action] = TraceView(trace).rewriteLocally(schema).getOrElse(Nil)

        {
          val preferUAVs = concated.collect {
            case v: PreferUAV => v
          }
            .distinct
          require(preferUAVs.size <= 1,
            s"attempt to dispatch ${preferUAVs.size} UAVs for a task," +
              " only 1 UAV can be dispatched for a task." +
              " (This behaviour is likely permanent and won't be fixed in the future)"
          )
        }

        val navs: Seq[UAVNavigation] = concated.collect {
          case nav: UAVNavigation => nav
        }

        val costs = navs.indices.map {
          i =>
            val c1 = intraCost(navs(i))
            val c2 = if (i >= navs.size - 1) 0
            else interCost(navs(i), navs(i + 1))
            c1 + c2
        }
        val sum = costs.sum

        sum
      }
    }

    override def _estimate(
                            getTrace: () => Trace,
                            schema: DataRowSchema
                          ): Double = {
      val trace = getTrace()
      new Instance(trace, schema).solve
    }
  }
}