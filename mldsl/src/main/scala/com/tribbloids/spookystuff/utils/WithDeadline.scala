package com.tribbloids.spookystuff.utils

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

object WithDeadline {

  val threadPool = Executors.newCachedThreadPool()
  val executionContext = ExecutionContext.fromExecutor(threadPool)
}
