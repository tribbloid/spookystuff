package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.uav.{UAVConf, UAVMetrics}

trait UAVAction extends Action {
  {
    UAVConf
    UAVMetrics
  }

  override def globalRewriters = super.globalRewriters :+ AutoTakeoffRule

//  def replaceAnchors(fn: PartialFunction[Anchor, Anchor]) = {
//    val result = doReplaceAnchors(fn)
//    result.injectFrom(this)
//    result
//  }
//
//  def doReplaceAnchors(fn: PartialFunction[Anchor, Anchor]): this.type = this
}
