package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.DataType
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

//right now it vaguely resembles SparkPlan in catalyst
//TODO: may subclass SparkPlan in the future to generate DataFrame directly, but not so fast
abstract class ExecutionPlan(
    val children: Seq[ExecutionPlan],
    val ec: SpookyExecutionContext
) extends TreeNode[ExecutionPlan]
    with Serializable {

  def this(
      children: Seq[ExecutionPlan]
  ) = this(
    children,
    children.map(_.ec).reduce(_ :++ _)
  )

  def spooky = ec.spooky
  def scratchRDDs = ec.scratchRDDs

  def verboseString = simpleString

  //Cannot be lazy, always defined on construction
  val schema: SpookySchema = SpookySchema(
    ec,
    fieldTypes = children
      .map(_.schema.fieldTypes)
      .reduceOption(_ ++ _)
      .getOrElse(ListMap[Field, DataType]())
  )

  implicit def withSchema(row: SquashedFetchedRow): SquashedFetchedRow#WSchema = row.WSchema(schema)

  def fieldMap: ListMap[Field, DataType] = schema.fieldTypes

  def allSortIndices: List[IndexedField] = schema.indexedFields.filter(_._1.self.isSortIndex)

  def firstChildOpt = children.headOption

  //beconRDD is always empty, with fixed partitioning, cogroup with it to maximize Local Cache hitting chance
  //by default, inherit from the first child
  protected final def inheritedBeaconRDDOpt =
    firstChildOpt.flatMap(_.beaconRDDOpt)

  lazy val beaconRDDOpt: Option[BeaconRDD[TraceView]] = inheritedBeaconRDDOpt

  def doExecute(): SquashedFetchedRDD

  final def execute(): SquashedFetchedRDD = {

    this.doExecute()
  }

  var storageLevel: StorageLevel = StorageLevel.NONE
  var _cachedRDD: SquashedFetchedRDD = _
  def cachedRDDOpt: Option[SquashedFetchedRDD] = Option(_cachedRDD)

  def isCached = cachedRDDOpt.nonEmpty

  final def broadcastAndRDD(): SquashedFetchedRDD = {
    spooky.rebroadcast()
    rdd()
  }

  // TODO: cachedRDD is redundant? just make it lazy val!
  final def rdd(): SquashedFetchedRDD = {
    cachedRDDOpt match {
      // if cached and loaded, use it
      case Some(cached) =>
        cached
      // if not cached, execute from upstream and use it.
      case None =>
        val exe = execute()
        val result = exe

        if (storageLevel != StorageLevel.NONE) {
          _cachedRDD = result.persist(storageLevel)
        }
        result
    }
  }

  def unsquashedRDD: RDD[FetchedRow] =
    rdd()
      .flatMap(v => v.WSchema(schema).unsquash)

  def persist[T](
      rdd: RDD[T],
      storageLevel: StorageLevel = ExecutionPlan.this.spooky.spookyConf.defaultStorageLevel
  ) = scratchRDDs.persist(rdd, storageLevel)

}
