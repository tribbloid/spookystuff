package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by peng on 10/07/17.
  */
//TODO: merge into DataRowSchema?
case class SpookyExecutionContext(
    spooky: SpookyContext,
    @transient scratchRDDs: ScratchRDDs = ScratchRDDs()
) {

  lazy val deployPluginsOnce: Unit = {
    try {
      spooky.Plugins.deployAll()
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

  def :++(b: SpookyExecutionContext): SpookyExecutionContext = {
    //    assert(this.spooky == b.spooky,
    //      "cannot merge execution plans due to diverging SpookyContext")

    val bb = b.scratchRDDs
    this.copy(
      scratchRDDs = scratchRDDs :++ bb
    )
  }

  def persist[T](
      rdd: RDD[T],
      storageLevel: StorageLevel = spooky.spookyConf.defaultStorageLevel
  ): RDD[T] = {

    scratchRDDs.persist(rdd, storageLevel)
  }
}
