package com.tribbloids.spookystuff.commons

import ai.acyclic.prover.commons.spark.serialization.BeforeAndAfterShipping
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.commons.lifespan.LocalCleanable

object WaitBeforeAppExit extends LocalCleanable {

  override def _lifespan: BeforeAndAfterShipping.Trigger[Lifespan.ActiveSparkApp.Internal] = Lifespan.ActiveSparkApp()

  @volatile var _waitBeforeExitDuration: Long = -1

  def waitBeforeExit(duration: Long): Unit = {
    _waitBeforeExitDuration = duration
  }

  /**
    * can only be called once
    */
  override protected def cleanImpl(): Unit = {
    if (_waitBeforeExitDuration > 0) {
      println(
        s"TEST FINISHED, waiting for ${_waitBeforeExitDuration}ms before termination ... (or you can terminate the process manually)"
      )

      Thread.sleep(_waitBeforeExitDuration)
    }
  }
}
