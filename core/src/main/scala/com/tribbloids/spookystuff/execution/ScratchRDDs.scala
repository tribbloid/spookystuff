package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.session.LocalCleanable
import com.tribbloids.spookystuff.utils.ShippingMarks
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

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
  def persist(rdd: RDD[_]): Unit = {

    rdd.persist()
    tempRDDs += rdd
  }

  def unpersist(df: DataFrame): Unit = {

    df.unpersist()
    tempDFs -= df
  }
  def unpersist(rdd: RDD[_]): Unit = {

    rdd.unpersist()
    tempRDDs -= rdd
  }

  def clearTables(): Unit = {
    tempTables.foreach {
      tuple =>
        tuple._2.sqlContext.dropTempTable(tuple._1)
    }
    tempTables.clear()
  }

  def clearAll(): Unit = {

    clearTables()
    tempDFs.foreach {
      df =>
        df.unpersist(false)
    }
    tempDFs.clear()
    tempRDDs.foreach {
      rdd =>
        rdd.unpersist(false)
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
