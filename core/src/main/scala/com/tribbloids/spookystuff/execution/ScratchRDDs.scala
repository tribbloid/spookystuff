package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.utils.ShippingMarks
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

/**
  * NOT serializable, can only run on driver
  */
object ScratchRDDs {
  val prefix = "temp_"

  val logger = LoggerFactory.getLogger(this.getClass)
  //TODO: this name should be validated against current DB to ensure that such name doesn't exist
  def tempTableName(): String = {
    prefix + Math.abs(Random.nextInt())
  }
}

case class ScratchRDDs(
    tempTables: ArrayBuffer[(String, Dataset[_])] = ArrayBuffer(),
    tempRDDs: ArrayBuffer[RDD[_]] = ArrayBuffer(),
    tempDSs: ArrayBuffer[Dataset[_]] = ArrayBuffer(),
    tempBroadcasts: ArrayBuffer[Broadcast[_]] = ArrayBuffer(),
    defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
) extends LocalCleanable
    with ShippingMarks {

  def registerTempView(
      ds: Dataset[_]
  ): String = {

    val existing = tempTables.find(_._2 == ds)

    existing match {
      case None =>
        val tempTableName = ScratchRDDs.tempTableName()
        ds.createOrReplaceTempView(tempTableName)
        tempTables += tempTableName -> ds
        tempTableName
      case Some(tuple) =>
        tuple._1
    }
  }

  def broadcast[T: ClassTag](sc: SparkContext)(
      v: T
  ): Broadcast[T] = {
    val result = sc.broadcast(v)
    tempBroadcasts += result
    result
  }

  def persistDS(
      ds: Dataset[_],
      storageLevel: StorageLevel = defaultStorageLevel
  ): Unit = {

    ds.persist(storageLevel)
    tempDSs += ds
  }

  def persist[T](
      rdd: RDD[T],
      storageLevel: StorageLevel = defaultStorageLevel
  ): RDD[T] = {

    if (rdd.getStorageLevel == StorageLevel.NONE) {
      tempRDDs += rdd.persist(storageLevel)
      ScratchRDDs.logger.debug(s"Persisting RDD : ${rdd.id}" )
    }
    rdd
  }

  def unpersistDS(
      ds: Dataset[_],
      blocking: Boolean = false
  ): Unit = {

    ds.unpersist(blocking)
    tempDSs -= ds
  }

  def unpersist(
      rdd: RDD[_],
      blocking: Boolean = false
  ): Unit = {

    rdd.unpersist(blocking)
    tempRDDs -= rdd
  }

  //TODO: add a utility function to make RDD to be uncached automatically once a number of downstream RDDs (can be filtered) are caculated.
  //This manual GC is important for some memory intensive workflow.

  def dropTempViews(): Unit = {
    tempTables.foreach { tuple =>
      ScratchRDDs.logger.debug(s"Dropping temperary view : ${tuple._1}")
      tuple._2.sqlContext.dropTempTable(tuple._1)
      ScratchRDDs.logger.debug(s"Temperary view  ${tuple._1} dropped")
    }
    tempTables.clear()
  }

  def clearAll(
      blocking: Boolean = false
  ): Unit = {

    ScratchRDDs.logger.debug("Starting Clean Up Process")

    dropTempViews()

    tempDSs.foreach { ds =>
      ds.unpersist(blocking)
      ScratchRDDs.logger.debug(s"Unpersisting temporary dataset ")
    }
    tempDSs.clear()

    tempRDDs.foreach { rdd =>
      rdd.unpersist(blocking)
    }
    tempRDDs.clear()

    tempBroadcasts.foreach { b =>
      ScratchRDDs.logger.debug(s"Destroying broadcast variable : ${b.id}")
      b.destroy()
      ScratchRDDs.logger.debug(s"Broadcast variable : ${b.id} destroyed")
    }
    tempBroadcasts.clear()
  }

  def <+>[T](b: ScratchRDDs, f: ScratchRDDs => ArrayBuffer[T]): ArrayBuffer[T] = {
    f(this) ++ f(b)
  }

  def ++(other: ScratchRDDs) = {
    this.copy(
      <+>(other, _.tempTables),
      <+>(other, _.tempRDDs),
      <+>(other, _.tempDSs),
      <+>(other, _.tempBroadcasts)
    )
  }

  override protected def cleanImpl(): Unit = {
    //    if (!isShipped) clearAll()
    // TODO: right now GC may clean it prematurely, disabled until better solution.
  }
}
