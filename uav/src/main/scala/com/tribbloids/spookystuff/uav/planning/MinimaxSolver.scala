package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.uav.dsl.GenPartitioners

trait MinimaxSolver extends Solver[GenPartitioners.MinimaxCost]

object MinimaxSolver {

  def JSprit = JSpritSolver
  def DRL = DRLSolver
}