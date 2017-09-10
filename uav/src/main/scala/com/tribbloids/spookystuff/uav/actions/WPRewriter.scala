package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.uav.planning.Takeoff

/**
  * replace
  */
object WPRewriter extends Rewriter[Trace] {

  override def rewrite(v1: Trace, ec: ExecutionContext): Trace = {

    val firstNavOpt = v1.find(_.isInstanceOf[UAVNavigation])

    firstNavOpt match {
      case Some(firstNav) =>
        v1.flatMap {
          case v if v == firstNav => Seq(Takeoff(), v)
          case v@ _ => Seq(v)
        }
      case None =>
        v1
    }
  }
}
