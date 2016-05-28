package com.tribbloids.spookystuff.row

import java.util.UUID

import com.tribbloids.spookystuff.actions.{Actions, Trace}
import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.execution.SchemaContext
import com.tribbloids.spookystuff.extractors.Resolved
import com.tribbloids.spookystuff.{SpookyContext, dsl}

import scala.collection.mutable.ArrayBuffer

object SquashedFetchedRow {

  def apply(data: Map[Field, Any]): SquashedFetchedRow = SquashedFetchedRow(
    dataRows = Array(DataRow(data.mapValues(_.asInstanceOf[Object])))
  )

  def withDocs(
             dataRows: Array[DataRow] = Array(DataRow()),
             docs: Seq[Fetched] = null
           ): SquashedFetchedRow = SquashedFetchedRow(
    dataRows = dataRows,
    lazyDocs = LazyDocs(docs = docs)
  )

  lazy val blank: SquashedFetchedRow = SquashedFetchedRow(Array(DataRow()))
}

/**
  * the main data structure in execution plan, representing:
  * 1. several DataRows combined together
  * 2. several arrays of abstract pages.
  * any extractions will be casted into applying to cartesian products of the above two
  * this is due to the fact that 90% of time is spent on fetching. < 5% on parsing & extraction.
  * WARNING: comparing to 0.3.x support for different join types has been discarded, costs too much memory.
  */
case class SquashedFetchedRow(
                               dataRows: Array[DataRow] = Array(),
                               lazyDocs: LazyDocs = LazyDocs() // TODO: change to Array to facilitate more join types
                             ) {

  import dsl._

  def ++ (another: SquashedFetchedRow) = {
    this.copy(dataRows = this.dataRows ++ another.dataRows)
  }

  def flattenData(
                   field: Field,
                   ordinalKey: Field,
                   left: Boolean,
                   sampler: Sampler[Any]
                 ): SquashedFetchedRow = {

    this.copy(dataRows = this.dataRows.flatMap(_.flatten(field, ordinalKey, left, sampler)))
  }

  def remove(fields: Field*) = this.copy(
    dataRows = dataRows.map(_.--(fields))
  )

  class W(schema: SchemaContext) extends Serializable {

    val lazyDocs = new SquashedFetchedRow.this.lazyDocs.W(schema.spooky)

    // by default, make sure no pages with identical name can appear in the same group.
    // TODO: need tests!
    @transient lazy val defaultGroupedFetched: Array[Seq[Fetched]] = {
      val grandBuffer: ArrayBuffer[Seq[Fetched]] = ArrayBuffer()
      val buffer: ArrayBuffer[Fetched] = ArrayBuffer()
      lazyDocs.get().foreach {
        page =>
          if (buffer.exists(_.name == page.name)) {
            grandBuffer += buffer.toList
            buffer.clear()
          }
          buffer += page
      }
      grandBuffer += buffer.toList //always left, have at least 1 member
      buffer.clear()
      grandBuffer.toArray
    }

    def groupedFetched: Array[Seq[Fetched]] = groupedFetchedOption.getOrElse(defaultGroupedFetched)

    //outer: dataRows, inner: grouped pages
    def semiUnsquash: Array[Array[FetchedRow]] = dataRows.map{
      dataRow =>
        val groupID = UUID.randomUUID()
        groupedFetched.zipWithIndex.map {
          tuple =>
            val withGroupID = dataRow.copyWithArgs(
              groupID = Some(groupID),
              groupIndex = tuple._2
            )
            FetchedRow(withGroupID, tuple._1: Seq[Fetched])
        }
    }

    // cartisian product
    def unsquash: Array[FetchedRow] = semiUnsquash.flatten

    /*
 * yield 1 SquashedPageRow, however the size of dataRows may increase according to the following rules:
 * each dataRow yield >= 1 dataRows.
 * each dataRow yield <= {groupedFetched.size} dataRows.
 * if a groupedFetched doesn't yield any new data it is omitted
 * if 2 groupedFetched yield identical results only the first is preserved?
 */
    //TODO: special optimization for Expression that only use pages
    //TODO: test redundant unchanged row elimination mechanism
    private def _extract(
                          exs: Seq[Resolved[Any]],
                          filterEmpty: Boolean = true,
                          distinct: Boolean = true
                          //set to true to ensure that repeated use of an alias (e.g. A for defaultJoinKey) always evict existing values to avoid data corruption
                        ): SquashedFetchedRow = {

      val allUpdatedDataRows: Array[DataRow] = semiUnsquash.flatMap {
        PageRows => //each element contains a different page group, CAUTION: not all of them are used: page group that yield no new datum will be removed, if all groups yield no new datum at least 1 row is preserved
          val dataRow_KVOpts = PageRows.map {
            pageRow =>
              val dataRow = pageRow.dataRow
              val KVOpts: Seq[(Field, Option[Any])] = exs.flatMap {
                expr =>
                  val resolving = expr.field.conflictResolving
                  val k = expr.field
                  val vOpt = expr.lift.apply(pageRow)
                  resolving match {
                    case Field.Remove => Some(k -> vOpt)
                    case _ => vOpt.map(v => k -> Some(v))
                  }
              }
              dataRow -> KVOpts
          }

          val filteredDataRow_KVOpts = if (!filterEmpty) dataRow_KVOpts
          else {
            val filtered = dataRow_KVOpts.filter(_._2.exists(_._2.nonEmpty))
            if (filtered.isEmpty) dataRow_KVOpts.headOption.toArray
            else filtered
          }
          val distinctDataRow_KVOpts = if (!distinct) filteredDataRow_KVOpts
          else {
            filteredDataRow_KVOpts.groupBy(_._2).map(_._2.head).toArray
          }

          val updatedDataRows: Array[DataRow] = distinctDataRow_KVOpts.map {
            tuple =>
              val K_VOrRemoves = tuple._2
              val dataRow = tuple._1
              val newKVs = K_VOrRemoves.collect{
                case (field, Some(v)) => field -> v
              }
              val removeKs = K_VOrRemoves.collect{
                case (field, None) => field
              }
              val updatedDataRow = dataRow ++ newKVs -- removeKs

              updatedDataRow
          }

          updatedDataRows
      }
      SquashedFetchedRow.this.copy(dataRows = allUpdatedDataRows)
    }

    def extract(ex: Resolved[Any]*) = _extract(ex)

    /*
 * same as extract + toTuple
 * each dataRow yield >= {effectiveTraces.size} traces.
 * each dataRow yield <= {groupedFetched.size * effectiveTraces.size} traces.
 * if a groupedFetched doesn't yield any trace it is omitted
 * if 2 groupedFetched yield identical traces only the first is preserved?
 */
    def interpolate(
                     effectiveTraces: Set[Trace],
                     spooky: SpookyContext,

                     filterEmpty: Boolean = true,
                     distinct: Boolean = true
                   ): Array[(Trace, DataRow)] = {

      val dataRows_traceOpts = semiUnsquash.flatMap {
        rows => //each element contains a different page group, CAUTION: not all of them are used: page group that yield no new datum will be removed, if all groups yield no new datum at least 1 row is preserved
          val dataRows_traceOpts = rows.flatMap {
            row =>
              effectiveTraces.map {
                trace =>
                  row.dataRow.clearWeakValues -> trace.interpolate(row, schema).map(_.children)
                //always discard old pages & temporary data before repartition, unlike flatten
              }
          }

          val filteredDataRows_traceOpts = if (!filterEmpty) dataRows_traceOpts
          else {
            val result = dataRows_traceOpts.filter(_._2.nonEmpty)
            if (result.isEmpty) dataRows_traceOpts.headOption.toArray
            else result
          }

          val mergedDataRows_traceOpts = if (!distinct) filteredDataRows_traceOpts
          else filteredDataRows_traceOpts.groupBy(_._2).map(_._2.head).toArray

          mergedDataRows_traceOpts
      }

      dataRows_traceOpts.map {
        v =>
          v._2.getOrElse(Actions.empty) -> v._1
      }
    }
  }

  @volatile var groupedFetchedOption: Option[Array[Seq[Fetched]]] = None
}