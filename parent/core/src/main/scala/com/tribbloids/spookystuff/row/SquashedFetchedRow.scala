package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.dsl.ForkType
import com.tribbloids.spookystuff.extractors.Resolved

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

object SquashedFetchedRow {

  def apply(data: Map[Field, Any]): SquashedFetchedRow = SquashedFetchedRow(
    dataRows = Array(DataRow(data))
  )

  def withDocs(
      dataRows: Array[DataRow] = Array(DataRow()),
      docs: Seq[Fetched] = null
  ): SquashedFetchedRow = SquashedFetchedRow(
    dataRows = dataRows,
    traceView = TraceView.withDocs(docs = docs)
  )

  lazy val blank: SquashedFetchedRow = SquashedFetchedRow(Array(DataRow()))
}

/**
  * the main data structure in execution plan, representing:
  *   1. several DataRows combined together 2. several arrays of abstract pages. any extractions will be casted into
  *      applying to cartesian products of the above two this is due to the fact that 90% of time is spent on fetching.
  *      < 5% on parsing & extraction.
  */
case class SquashedFetchedRow(
    dataRows: Array[DataRow] = Array(),
    traceView: TraceView = TraceView()
) {

  def ++(another: SquashedFetchedRow): SquashedFetchedRow = {
    this.copy(dataRows = this.dataRows ++ another.dataRows)
  }

  def explodeData(
      field: Field,
      ordinalKey: Field,
      forkType: ForkType,
      sampler: Sampler[Any]
  ): SquashedFetchedRow = {

    this.copy(
      dataRows = this.dataRows.flatMap(_.explode(field, ordinalKey, forkType, sampler))
    )
  }

  def remove(fields: Field*): SquashedFetchedRow = this.copy(
    dataRows = dataRows.map(_.--(fields))
  )

  case class WSchema(schema: SpookySchema) extends Serializable {

    @transient lazy val withSpooky: traceView.WithSpooky =
      new SquashedFetchedRow.this.traceView.WithSpooky(schema.spooky)

    // by default, make sure no pages with identical name can appear in the same group.
    // TODO: not well defined, switch to more manual option & re-slice with a function
    @transient lazy val fetchedSlices: Array[Seq[Fetched]] = {
      val grandBuffer: ArrayBuffer[Seq[Fetched]] = ArrayBuffer()
      val buffer: ArrayBuffer[Fetched] = ArrayBuffer()
      withSpooky.fetched.foreach { fetched =>
        if (buffer.exists(_.name == fetched.name)) {
          grandBuffer += buffer.toList
          buffer.clear()
        }
        buffer += fetched
      }
      grandBuffer += buffer.toList // always left, have at least 1 member
      buffer.clear()
      grandBuffer.toArray
    }

    // outer: dataRows, inner: grouped pages
    def semiUnSquash: Array[Array[FetchedRow]] = dataRows.map { dataRow =>
      val _id = UUID.randomUUID()
      val withGroupID = dataRow.copy(
        fastID = Some(_id)
      )

      fetchedSlices.zipWithIndex.map { tuple =>
        FetchedRow(withGroupID, tuple._1, tuple._2)
      }
    }

    // Cartesian product
    def unSquash: Array[FetchedRow] = semiUnSquash.flatten

    /**
      * yield 1 SquashedPageRow, but the size of dataRows may increase according to the following rules: each dataRow
      * yield >= 1 dataRows. each dataRow yield {groupedFetched.size} dataRows.
      */
    private def _extract(
        exs: Seq[Resolved[Any]],
        filterEmpty: Boolean = true
    ): SquashedFetchedRow = {

      val allUpdatedDataRows: Array[DataRow] = semiUnSquash.flatMap {
        rows => // each element contains a different page group, CAUTION: not all of them are used: page group that yield no new datum will be removed, if all groups yield no new datum at least 1 row is preserved
          val dataRow_KVOpts = rows.map { row =>
            val dataRow = row.dataRow
            val KVOpts: Seq[(Field, Option[Any])] = exs.flatMap { expr =>
              val resolving = expr.field.conflictResolving
              val k = expr.field
              val vOpt = expr.lift.apply(row)
              resolving match {
                case Field.Replace => Some(k -> vOpt)
                case _             => vOpt.map(v => k -> Some(v))
              }
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

          val updatedDataRows: Array[DataRow] = filteredDataRow_KVOpts.map { tuple =>
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
      SquashedFetchedRow.this.copy(dataRows = allUpdatedDataRows)
    }

    def extract(ex: Resolved[Any]*): SquashedFetchedRow = _extract(ex)

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

      val dataRows_traces = semiUnSquash.flatMap {
        rows => // each element contains a different page group, CAUTION: not all of them are used: page group that yield no new datum will be removed, if all groups yield no new datum at least 1 row is preserved
          val pairs = rows.flatMap { row =>
            traces.map { trace =>
              val rewritten: Seq[TraceView] = TraceView(trace)
                .interpolateAndRewriteLocally(row, schema)
                .filter(_.nonEmpty)
              row.dataRow -> rewritten
              // always discard old pages & temporary data before repartition, unlike flatten
            }
          }

          val filteredPairs =
            if (!filterEmpty) pairs
            else {
              val result = pairs.filter(_._2.nonEmpty)
              result
            }

          val distinctPairs =
            if (!distinct) filteredPairs
            else filteredPairs.groupBy(_._2).map(_._2.head).toArray

          distinctPairs
      }

      val result = dataRows_traces.flatMap { mergedDataRows_traces =>
        val traces = mergedDataRows_traces._2

        traces.map { trace =>
          TraceView(trace) -> mergedDataRows_traces._1
        }
      }

      result
    }

  }
}
