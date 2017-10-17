package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.UAVConf

/**
  * Do many things:
  * Globally
  * 1. add takeoff to the beginning of the trace if it is missing
  *
  * Locally
  * 2. replace Anchors.Home with UAVConf.home
  * 3. replace Anchors.HomeLevelProjection with previous action._end minus its relative altitude to UAVConf.home
  * 4. replace Anchors.MSLProjection with previous action._end minus its absolute altitude to Anchors.Geodetic
  * 5. (pending) replace Anchors.GroundProjection with previous action._end
  *     minus its relative altitude to ground elevation directly under it (query from various terrian API or DB)
  */
object UAVRewriteRule extends RewriteRule[Trace] {

  override def rewrite(v1: Trace, schema: DataRowSchema): Trace = {

    val uavConf = schema.ec.spooky.getConf[UAVConf]

    val navs = v1.collect {
      case nav: UAVNavigation => nav
    }

    val result = if (navs.isEmpty)
      v1
    else if (!navs.head.isInstanceOf[Takeoff])
      List(Takeoff()) ++ v1
    else
      v1

    result
  }

//  override def rewriteLocally(v1: Trace, schema: DataRowSchema): Some[Trace] = {
//
//    val uavConf = schema.ec.spooky.getConf[UAVConf]
//
//    var lastLocationOpt: Option[Location#WithHome] = None
//
//    val result = v1.map {
//      case uavAction: UAVAction =>
//        val replaced = uavAction.replaceAnchors {
//          case Anchors.Home =>
//            uavConf.home
//          case Anchors.HomeLevelProjection =>
//            lastLocationOpt.map(_.homeLevelProj).getOrElse(
//              Anchors.HomeLevelProjection
//            )
//          case Anchors.MSLProjection =>
//            lastLocationOpt.map(_.homeLevelProj).getOrElse(
//              Anchors.MSLProjection
//            )
//        }
//        replaced match {
//          case nav: UAVNavigation =>
//            lastLocationOpt = Some(nav._end.withHome(uavConf.home))
//          case _ =>
//        }
//        replaced
//      case other@ _ =>
//        other
//    }
//    Some(result)
//  }
}
