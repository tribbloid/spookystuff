package com.tribbloids.spookystuff.uav.planning.VRPOptimizers

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.dsl.Localitys

/**
  * Created by peng on 7/3/17.
  */
trait VRPFixture extends SpookyEnvFixture {

  var i = 1

  def getVRP: Localitys.VRP = {
    val solutionPath = s"log/JSprit/${this.getClass.getSimpleName}.$i.solution.png"
    val progressPath = s"log/JSprit/${this.getClass.getSimpleName}.$i.progress.png"
    i += 1
    Localitys.VRP(
      numUAVOverride = Some(this.parallelism),
      cohesiveness = 0,
      solutionPlotPathOpt = Some(solutionPath),
      covergencePlotPathOpt = Some(progressPath)
    )
  }
}
