package com.tribbloids.spookystuff.tool

import com.tribbloids.spookystuff.actions.MayTimeout
import com.tribbloids.spookystuff.agent.Harness
import com.tribbloids.spookystuff.commons.CommonUtils
import org.slf4j.LoggerFactory

trait Tool[
    R // result
] extends Invocation[R] {

  protected def withTimeoutDuring[T](agent: Harness)(f: => T): T = {

    var baseStr: String = LoggerPrefix(agent)
    this match {
      case timed: MayTimeout =>
        val timeout = timed.getTimeout(agent)

        baseStr = baseStr + s" in ${timeout}"
        LoggerFactory.getLogger(this.getClass).info(this.withDetail(baseStr))

        agent.progress.ping()

        // the following execute f in a different thread, thus `timed` has to be declared as `ThreadSafe`
        CommonUtils.withTimeout(timeout.hardTerimination)(
          f,
          agent.progress.defaultHeartbeat
        )
      case _ =>
        LoggerFactory.getLogger(this.getClass).info(this.withDetail(baseStr))

        f
    }
  }

  final def exe(agent: Harness): R = {
    withTimeoutDuring(agent) {
      doExe(agent)
    }
  }

  def doExe(agent: Harness): R
}
