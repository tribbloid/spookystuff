package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.util.Try

case class ExecutionContext(
    ctx: SpookyContext,
    @transient tempRefs: TemporaryRefs = TemporaryRefs()
) {

  lazy val deployPluginsOnce: Unit = {
    try {
      ctx.Plugins.deployAll()
    } catch {
      case e: Throwable =>
        LoggerFactory
          .getLogger(this.getClass)
          .error("Deployment of some plugin(s) has failed", e)
    }
  }

  def tryDeployPlugin(): Try[Unit] = {
    Try(deployPluginsOnce)
  }

  def :++(b: ExecutionContext): ExecutionContext = {
    //    assert(this.spooky == b.spooky,
    //      "cannot merge execution plans due to diverging SpookyContext")

    val bb = b.tempRefs
    this.copy(
      tempRefs = tempRefs :++ bb
    )
  }

  def persist[T](
      rdd: RDD[T],
      storageLevel: StorageLevel = ctx.conf.defaultStorageLevel
  ): RDD[T] = {

    tempRefs.persist(rdd, storageLevel)
  }
}
