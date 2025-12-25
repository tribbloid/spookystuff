package com.tribbloids.spookystuff.tool

import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.actions.HasTrace
import com.tribbloids.spookystuff.doc.Observation

object Invocation {}

trait Invocation[
    R // result
] extends HasTrace {

  def exe(agent: Agent): Seq[Observation]

  protected def LoggerPrefix[T](agent: Agent): String = {
    s"[${agent.taskContextOpt.map(_.partitionId()).getOrElse(0)}]+> ${this.toString}"
  }

}
