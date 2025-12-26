package com.tribbloids.spookystuff.tool

import com.tribbloids.spookystuff.agent.Harness
import com.tribbloids.spookystuff.tool.HasTrace

object Invocation {}

trait Invocation[
    R // result
] extends HasTrace {

  def exe(agent: Harness): R

//  def dryRun(agent: Agent): R

  protected def LoggerPrefix[T](agent: Harness): String = {
    s"[${agent.taskContextOpt.map(_.partitionId()).getOrElse(0)}]+> ${this.toString}"
  }

}
