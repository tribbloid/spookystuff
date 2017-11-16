package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.session.LocalCleanable
import com.tribbloids.spookystuff.utils.ShippingMarks
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * NOT serializable, can only run on driver
  */
object ScratchRDDs {
  val prefix = "temp_"

  def tempTableName(): String = {
    prefix + Math.abs(Random.nextInt())
  }
}

case class ScratchRDDs(
                        tempTables: ArrayBuffer[(String, DataFrame)] = ArrayBuffer(),
                        tempRDDs: ArrayBuffer[RDD[_]] = ArrayBuffer(),
                        tempDFs: ArrayBuffer[DataFrame] = ArrayBuffer()
                      ) extends LocalCleanable with ShippingMarks {

  def register(
                df: DataFrame
              ): String = {

    val existing = tempTables.find(_._2 == df)

    existing match {
      case None =>
        val tempTableName = ScratchRDDs.tempTableName()
        df.registerTempTable(tempTableName)
        tempTables += tempTableName -> df
        tempTableName
      case Some(tuple) =>
        tuple._1
    }
  }

  def persist(df: DataFrame): Unit = {

    df.persist()
    tempDFs += df
  }
  def persist[T](
                  rdd: RDD[T],
                  storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                ): RDD[T] = {

    if (rdd.getStorageLevel == StorageLevel.NONE) {
      tempRDDs += rdd.persist(storageLevel)
    }
    rdd
  }

  def unpersistDF(
                   df: DataFrame,
                   blocking: Boolean = false
                 ): Unit = {

    df.unpersist(blocking)
    tempDFs -= df
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

  def clearTables(): Unit = {
    tempTables.foreach {
      tuple =>
        tuple._2.sqlContext.dropTempTable(tuple._1)
    }
    tempTables.clear()
  }

  def clearAll(
                blocking: Boolean = false
              ): Unit = {

    clearTables()
    tempDFs.foreach {
      df =>
        df.unpersist(blocking)
    }
    tempDFs.clear()
    tempRDDs.foreach {
      rdd =>
        rdd.unpersist(blocking)
    }
    tempRDDs.clear()
  }

  def <+>[T](b: ScratchRDDs, f: ScratchRDDs => ArrayBuffer[T]): ArrayBuffer[T] = {
    f(this) ++ f(b)
  }

  def ++(other: ScratchRDDs) = {
    this.copy(
      <+>(other, _.tempTables),
      <+>(other, _.tempRDDs),
      <+>(other, _.tempDFs)
    )
  }

  override protected def cleanImpl(): Unit = {
    //    if (!isShipped) clearAll()
    // TODO: right now GC may clean it prematurely, disabled until better solution.
  }
}
