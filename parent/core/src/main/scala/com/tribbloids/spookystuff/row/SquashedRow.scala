package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.{Actions, Trace, TraceView}
import com.tribbloids.spookystuff.doc.DocOption
import com.tribbloids.spookystuff.extractors.Expr

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object SquashedRow {

//  def apply(data: Map[Field.Symbol, Any]): SquashedRow = SquashedRow(
//    dataRows = Array(DataRow(data))
//  )

//  def fromKV(
//              kvs: Iterable[(Field, Any)]
//            ): SquashedRow = {
//
//    val dataRow = DataRow.fromKV(kvs).outer
//
//    withDocs(
//      dataRows = Array(dataRow)
//    )
//  }

  def withDocs(
      dataRows: Array[DataRow] = Array(DataRow()),
      docs: Seq[DocOption] = Nil
  ): SquashedRow = SquashedRow(
    dataRows = dataRows,
    traceView = TraceView.withDocs(docs = docs)
  )

  lazy val blank: SquashedRow = SquashedRow(Array(DataRow()))
}

/**
  * the main data structure in execution plan, representing:
  *   1. several DataRows combined together 2. several arrays of abstract pages. any extractions will be casted into
  *      applying to cartesian products of the above two this is due to the fact that 90% of time is spent on fetching.
  *      < 5% on parsing & extraction.
  */
case class SquashedRow(
    dataRows: Array[DataRow] = Array(),
    traceView: TraceView = TraceView()
) {

  def mapPerDataRow(fn: DataRow => DataRow): SquashedRow = {
    this.copy(dataRows = this.dataRows.map(fn))
  }

  case class WSchema(schema: SpookySchema) extends Serializable {

    val withSpooky: traceView.WithSpooky = new SquashedRow.this.traceView.WithSpooky(schema.spooky)

    @volatile var groupedDocsOverride: Option[Array[Seq[DocOption]]] = None

    def groupedDocs: Array[Seq[DocOption]] = groupedDocsOverride.getOrElse(defaultGroupedFetched)

    // by default, make sure no pages with identical name can appear in the same group.
    // TODO: need tests!
    @transient lazy val defaultGroupedFetched: Array[Seq[DocOption]] = {
      val grandBuffer: ArrayBuffer[Seq[DocOption]] = ArrayBuffer()
      val buffer: ArrayBuffer[DocOption] = ArrayBuffer()
      withSpooky.getDoc.foreach { page =>
        if (buffer.exists(_.name == page.name)) {
          grandBuffer += buffer.toList
          buffer.clear()
        }
        buffer += page
      }
      grandBuffer += buffer.toList // always left, have at least 1 member
      buffer.clear()
      grandBuffer.toArray
    }

    // outer: dataRows, inner: grouped pages
    def semiUnsquash: Array[Array[FetchedRow]] = dataRows.map { dataRow =>
      val groupID = UUID.randomUUID()
      groupedDocs.zipWithIndex.map { tuple =>
        val withGroupID = dataRow.copy(
          groupID = Some(groupID),
          groupIndex = tuple._2
        )
        FetchedRow(withGroupID, tuple._1: Seq[DocOption])
      }
    }

    // cartisian product
    def unsquash: Array[FetchedRow] = semiUnsquash.flatten

    /**
      * yield 1 SquashedPageRow, but the size of dataRows may increase according to the following rules: each dataRow
      * yield >= 1 dataRows. each dataRow yield <= {groupedFetched.size} dataRows. if a groupedFetched doesn't yield any
      * new data it is omitted if 2 groupedFetched yield identical results only the first is preserved? TODO: need more
      * test on this one handling of previous values with identical field id is determined by new
      * Field.conflictResolving.
      */
    // TODO: special optimization for Expression that only use pages
    private def _extract(
        exs: Seq[Expr[Any]],
        filterEmpty: Boolean = true,
        distinct: Boolean = true
    ): SquashedRow = {

      val allUpdatedDataRows: Array[DataRow] = semiUnsquash.flatMap {
        PageRows => // each element contains a different page group, CAUTION: not all of them are used: page group that yield no new datum will be removed, if all groups yield no new datum at least 1 row is preserved
          val dataRow_KVOpts = PageRows.map { pageRow =>
            val dataRow = pageRow.dataRow
            val KVOpts: Seq[(Alias, Option[Any])] = exs.flatMap { expr =>
              val k = expr.alias
              val vOpt = expr.lift.apply(pageRow)
              Some(k -> vOpt)
            }
            dataRow -> KVOpts
          }

          val filteredDataRow_KVOpts =
            if (!filterEmpty) dataRow_KVOpts
            else {
              val filtered = dataRow_KVOpts.filter(_._2.exists(_._2.nonEmpty))
              if (filtered.isEmpty) dataRow_KVOpts.headOption.toArray
              else filtered
            }
          val distinctDataRow_KVOpts =
            if (!distinct) filteredDataRow_KVOpts
            else {
              filteredDataRow_KVOpts.groupBy(_._2).map(_._2.head).toArray
            }

          val updatedDataRows: Array[DataRow] = distinctDataRow_KVOpts.map { tuple =>
            val K_VOrRemoves = tuple._2
            val dataRow = tuple._1
            val newKVs = K_VOrRemoves.collect {
              case (field, Some(v)) => field -> v
            }
            val removeKs = K_VOrRemoves.collect {
              case (field, None) => field
            }
            val updatedDataRow = dataRow ++ newKVs -- removeKs

            updatedDataRow
          }

          updatedDataRows
      }
      SquashedRow.this.copy(dataRows = allUpdatedDataRows)
    }

    def extract(ex: Expr[Any]*): SquashedRow = _extract(ex)

    /*
     * same as extract + toTuple
     * each dataRow yield >= {effectiveTraces.size} traces.
     * each dataRow yield <= {groupedFetched.size * effectiveTraces.size} traces.
     * if a groupedFetched doesn't yield any trace it is omitted
     * if 2 groupedFetched yield identical traces only the first is preserved?
     */
    def interpolateAndRewriteLocally(
        traces: Set[Trace],
        filterEmpty: Boolean = true,
        distinct: Boolean = true
    ): Array[(TraceView, DataRow)] = {

      val dataRows_traces = semiUnsquash.flatMap {
        rows => // each element contains a different page group, CAUTION: not all of them are used: page group that yield no new datum will be removed, if all groups yield no new datum at least 1 row is preserved
          val dataRows_traces = rows.flatMap { row =>
            traces.map { trace =>
              val rewritten: Seq[TraceView] = TraceView(trace).interpolateAndRewriteLocally(row, schema)
              row.dataRow -> rewritten
            // always discard old pages & temporary data before repartition, unlike flatten
            }
          }

          val filteredDataRows_traces =
            if (!filterEmpty) dataRows_traces
            else {
              val result = dataRows_traces.filter(_._2.nonEmpty)
              if (result.isEmpty) dataRows_traces.headOption.toArray
              else result
            }

          val mergedDataRows_traces =
            if (!distinct) filteredDataRows_traces
            else filteredDataRows_traces.groupBy(_._2).map(_._2.head).toArray

          mergedDataRows_traces
      }

      dataRows_traces.flatMap { v =>
        val traces = v._2
        if (traces.isEmpty) {
          Seq(TraceView(Actions.empty) -> v._1)
        } else {
          traces.map { trace =>
            TraceView(trace) -> v._1
          }
        }
      }
    }
  }
}
