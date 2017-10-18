package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.{RewriteRule, Trace}
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.actions.{Takeoff, UAVNavigation}

object InsertPrevNavRule extends RewriteRule[Trace] {
  /**
    * @param v
    * @param schema
    * @return
    */
  override def rewrite(v: Trace, schema: DataRowSchema) = {
    var prevNav: UAVNavigation = null
    val rewritten = v.map {
      case vv: Takeoff =>
        vv.copy(
          prevNavOpt = vv.prevNavOpt.orElse(Option(prevNav))
        )
      case vv: UAVNavigation =>
        prevNav = vv
        vv
      case vv@ _ =>
        vv
    }

    rewritten
  }
}
