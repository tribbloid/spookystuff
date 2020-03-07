package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}

object WaitBeforeAppExit extends LocalCleanable {

  override def _lifespan: Lifespan = Lifespan.SparkApp()

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
