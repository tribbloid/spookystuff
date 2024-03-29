package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.agent.Agent

import scala.concurrent.duration.Duration
import scala.util.Random

/**
  * Wait for some time
  *
  * @param cooldown
  *   seconds to be wait for
  */
@SerialVersionUID(-4852391414869985193L)
case class Delay(
    override val cooldown: Duration = Const.Interaction.delayMax
) extends Interaction
    with Driverless {

  override def exeNoOutput(agent: Agent): Unit = {
    // do nothing
  }
}

object Delay {

  /**
    * Wait for some random time, add some unpredictability
    *
    * @param cooldown
    *   seconds to be wait for
    */
  @SerialVersionUID(2291926240766143181L)
  case class RandomDelay(
      override val cooldown: Duration = Const.Interaction.delayMin,
      maxDelay: Duration = Const.Interaction.delayMax
  ) extends Interaction
      with Driverless {

    assert(maxDelay >= cooldown)

    override def exeNoOutput(agent: Agent): Unit = {
      Thread.sleep(Random.nextInt((maxDelay - cooldown).toMillis.toInt))
    }
  }

}
