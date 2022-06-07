package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.{RewriteRule, Trace, TraceView}
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.actions.{Takeoff, UAVNavigation}

object EstimateLocationRule extends RewriteRule[Trace] {

  /**
    * @param v
    * @param schema
    * @return
    */
  override def rewrite(v: Trace, schema: SpookySchema) = {

    _rewritePartition(Iterator(TraceView(v) -> Unit), schema)
      .map(_._1.children)
      .flatten
      .toList
  }

  def _rewritePartition[V](
      vs: Iterator[(TraceView, V)],
      schema: SpookySchema
  ): Iterator[(TraceView, V)] = {

    var prevNav: UAVNavigation = null
    val rewritten = vs.map { v =>
      val rr = v._1.children.map {
        case vv: Takeoff =>
          val result = vv.copy(
            prevNavOpt = Option(prevNav)
          )
          prevNav = result
          result
        case vv: UAVNavigation =>
          prevNav = vv
          vv
        case vv @ _ =>
          vv
      }
      TraceView(rr) -> v._2
    }

    rewritten
  }
}
