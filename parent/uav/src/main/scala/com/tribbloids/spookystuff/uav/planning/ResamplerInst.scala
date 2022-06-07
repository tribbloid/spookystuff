package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.Trace

trait ResamplerInst {

  def apply(v: Map[Int, Seq[Trace]]): Map[Int, Seq[Trace]]
}
