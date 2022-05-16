package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.IDMixin
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager

trait HadoopTypes {

  trait HadoopType extends ElementaryType {}

  case class HadoopShutdown(priority: Int) extends HadoopType {

    case class ID(id: Int) extends IDMixin.ForProduct {
      override def toString: String = s"JVM-$id"
    }

    override protected def _batchID(ctx: LifespanContext): ID =
      ID((ctx.thread.getId % Lifespan.JVM.MAX_NUMBER_OF_SHUTDOWN_HOOKS).toInt)

    override protected def _registerHook(ctx: LifespanContext, fn: () => Unit): Unit = {
      val hookTask = new Runnable() {
        override def run(): Unit = fn()
      }

      try {
        ShutdownHookManager
          .get()
          .addShutdownHook(
            hookTask,
            priority
          )
      } catch {
        case e: IllegalStateException if e.getMessage.contains("Shutdown") =>
        // DO NOTHING
      }
    }

  }

  object HadoopShutdown {

    object BeforeSpark extends HadoopShutdown(FileSystem.SHUTDOWN_HOOK_PRIORITY + 60)
  }
}
