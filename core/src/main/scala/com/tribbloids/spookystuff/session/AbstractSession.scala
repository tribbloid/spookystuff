package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.utils.io.Progress
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.apache.spark.TaskContext

import java.util.Date
import scala.collection.mutable.ArrayBuffer

abstract class AbstractSession(val spooky: SpookyContext) extends LocalCleanable {

  spooky.spookyMetrics.sessionInitialized += 1
  val startTime: Long = new Date().getTime
  val backtrace: ArrayBuffer[Action] = ArrayBuffer()

  def taskContextOpt: Option[TaskContext] = lifespan.ctx.taskOpt

  lazy val progress: Progress = Progress()
}
