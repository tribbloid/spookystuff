package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Observation

trait Named extends MayExport with Product {

  def name: String = this.toString
}

object Named {

  case class Explicitly[T <: MayExport](
      delegate: T,
      override val name: String
  ) extends Named {

    final override def outputNames: Set[String] = Set(name)

    override protected[actions] def doExe(agent: Agent): Seq[Observation] = {

      val result = delegate.doExe(agent).map { observation =>
        observation.updated(
          uid = observation.uid.copy()(name = name)
        )
      }
      result
    }

    override def injectFrom(same: ActionLike): Unit = {
      same match {
        case _same: Explicitly[_] =>
          delegate.injectFrom(_same.delegate)
      }
    }

    override def skeleton: Option[T] = Some(delegate)
  }
}
