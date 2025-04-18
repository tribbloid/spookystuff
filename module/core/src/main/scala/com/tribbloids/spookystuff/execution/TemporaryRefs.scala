package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.commons.lifespan.LocalCleanable
import com.tribbloids.spookystuff.utils.ShippingMarks
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

/**
  * NOT serializable, can only run on driver
  */
object TemporaryRefs {
  val prefix: String = "temp_"

  // TODO: this name should be validated against current DB to ensure that such name doesn't exist
  def nextTempTableName(): String = {
    prefix + Math.abs(Random.nextInt())
  }

  type ScopeID = String

//  type InScope = Scoped#InScope

//  case class Scoped( // TODO: remove, useless
//      root: TemporaryRefs
//  ) {
//
//    val children: mutable.HashMap[Set[ScopeID], TemporaryRefs] = mutable.HashMap.empty
//
//    @transient var activeScopeIDs: Set[ScopeID] = Set.empty
//
//    def setActive(scopes: ScopeID*): Unit = {
//
//      activeScopeIDs = scopes.toSet
//    }
//
//    case class InScope(scopeIDs: Set[ScopeID]) {
//
//      def get: TemporaryRefs = {
//
//        if (scopeIDs.isEmpty) root
//        else {
//          children.getOrElseUpdate(
//            scopeIDs,
//            TemporaryRefs(defaultStorageLevel = root.defaultStorageLevel)
//          )
//        }
//
//      }
//
//      def filter: Seq[(Set[ScopeID], TemporaryRefs)] = {
//
//        if (scopeIDs.isEmpty) Seq(Set.empty[ScopeID] -> root)
//        else {
//
//          children.filterKeys { ids =>
//            ids.intersect(scopeIDs).nonEmpty
//          }.toSeq
//        }
//
//      }
//
//      def clearAll(blocking: Boolean = false): Unit = {
//
//        val filtered = filter
//
//        filtered.foreach {
//          case (_, v) =>
//            v.clearAll(blocking)
//        }
//
//        filter.foreach {
//          case (k, _) =>
//            children.remove(k)
//        }
//      }
//    }
//
//    def active: InScope = InScope(activeScopeIDs)
//
//    def clearAll(blocking: Boolean = false): Unit = {
//
//      children.foreach {
//        case (_, v) =>
//          v.clearAll(blocking)
//      }
//
//      children.clear()
//
//      root.clearAll(blocking)
//    }
//
//    def :++(that: Scoped): Scoped = {
//      val result = Scoped(this.root :++ that.root)
//
//      val keys = this.children.keySet ++ that.children.keySet
//
//      for (key <- keys) {
//
//        result.children += key -> (this.children.get(key).toSeq ++ that.children.get(key)).reduce(_ :++ _)
//      }
//
//      result
//    }
//  }
//
//  object Scoped {
//
//    implicit def fromRoot(v: TemporaryRefs): Scoped = Scoped(v)
//  }

}

case class TemporaryRefs( // TODO: should be "RefCounting"
    tempTables: ArrayBuffer[(String, Dataset[?])] = ArrayBuffer(),
    tempRDDs: ArrayBuffer[RDD[?]] = ArrayBuffer(),
    tempDSs: ArrayBuffer[Dataset[?]] = ArrayBuffer(), // TODO: merge into tempTables
    tempBroadcasts: ArrayBuffer[Broadcast[?]] = ArrayBuffer(),
    defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
) extends LocalCleanable
    with ShippingMarks {

  def registerTempView(
      ds: Dataset[?]
  ): String = {

    val existing = tempTables.find(_._2 == ds)

    existing match {
      case None =>
        val tempTableName = TemporaryRefs.nextTempTableName()
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

  def persist[T](
      rdd: RDD[T],
      storageLevel: StorageLevel = defaultStorageLevel
  ): RDD[T] = {

    if (rdd.getStorageLevel == StorageLevel.NONE) {
      tempRDDs += rdd.persist(storageLevel)
    }
    rdd
  }

  def unpersist(
      rdd: RDD[?],
      blocking: Boolean = false
  ): Unit = {

    rdd.unpersist(blocking)
    tempRDDs -= rdd
  }

  object datasets {

    def persist(
        ds: Dataset[?],
        storageLevel: StorageLevel = defaultStorageLevel
    ): Unit = {

      ds.persist(storageLevel)
      tempDSs += ds
    }

    def unpersist(
        ds: Dataset[?],
        blocking: Boolean = false
    ): Unit = {

      ds.unpersist(blocking)
      tempDSs -= ds
    }
  }

  // TODO: add a utility function to make RDD to be uncached automatically once a number of downstream RDDs (can be filtered) are caculated.
  // This manual GC is important for some memory intensive workflow.

  def dropTempViews(): Unit = {
    tempTables.foreach { tuple =>
      tuple._2.sqlContext.dropTempTable(tuple._1)
    }
    tempTables.clear()
  }

  def clearAll(
      blocking: Boolean = false
  ): Unit = {

    dropTempViews()

    tempDSs.foreach { ds =>
      ds.unpersist(blocking)
    }
    tempDSs.clear()

    tempRDDs.foreach { rdd =>
      rdd.unpersist(blocking)
    }
    tempRDDs.clear()

    tempBroadcasts.foreach { b =>
      b.destroy()
    }
    tempBroadcasts.clear()
  }

  protected def <+>[T](b: TemporaryRefs, f: TemporaryRefs => ArrayBuffer[T]): ArrayBuffer[T] = {
    f(this) ++ f(b)
  }

  def :++(that: TemporaryRefs): TemporaryRefs = {
    this.copy(
      <+>(that, _.tempTables),
      <+>(that, _.tempRDDs),
      <+>(that, _.tempDSs),
      <+>(that, _.tempBroadcasts)
    )
  }

  override protected def cleanImpl(): Unit = {
    //    if (!isShipped) clearAll()
    // TODO: right now GC may clean it prematurely, disabled until better solution.
  }
}
