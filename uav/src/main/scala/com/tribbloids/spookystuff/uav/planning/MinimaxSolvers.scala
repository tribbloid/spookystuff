package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.uav.planning.minimax.{DRLSolver, JSpritSolver}

object MinimaxSolvers {

  def JSprit = JSpritSolver
  def DRL = DRLSolver
}
