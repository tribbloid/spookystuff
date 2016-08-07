package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.caching.{ConcurrentMap, ExploreRunnerCache}
import com.tribbloids.spookystuff.dsl.ExploreAlgorithms.ExploreImpl
import com.tribbloids.spookystuff.extractors.Resolved
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.NOTSerializableMixin
import com.tribbloids.spookystuff.{SpookyContext, dsl}

import scala.language.implicitConversions

// use Array to minimize serialization footage
case class Open_Visited(
                         open: Option[Array[DataRow]] = None,
                         visited: Option[Array[DataRow]] = None
                       )

/**
  * NOT serializable: expected to be constructed on Executors
  */
class ExploreRunner(
                     val itr: Iterator[(TraceView, Open_Visited)], //TODO: change to TraceViewView
                     val executionID: Long
                   ) extends NOTSerializableMixin {

  import dsl._

  //TODO: add fast sort implementation
  val open: ConcurrentMap[TraceView, Iterable[DataRow]] = ConcurrentMap() //TODO: Change to ConcurrentMap[TraceView, Array[DataRow]]

  //  val openVs = mutable.SortedSet[(TraceView, Array[DataRow])] = mutable.SortedSet()
  val visited: ConcurrentMap[TraceView, Iterable[DataRow]] = ConcurrentMap()

  itr.foreach{
    tuple =>
      tuple._2.open.map(v => open += tuple._1 -> v)
      tuple._2.visited.map(v => visited += tuple._1 -> v)
  }

  ExploreRunnerCache.register(this)

  protected def commitIntoVisited(
                                   key: TraceView,
                                   value: Iterable[DataRow],
                                   reducer: RowReducer
                                 ): Unit = {
    val oldVs: Iterable[DataRow] = visited.getOrElse(key, Nil)
    val newVs = reducer(value, oldVs)
    visited.put(key, newVs)
  }

  protected def executeOnce(
                             resolved: Resolved[Any],
                             sampler: Sampler[Any],
                             joinType: JoinType,

                             trace: Set[Trace]
                           )(
                             impl: ExploreImpl,
                             `depth_++`: Resolved[Int],
                             spooky: SpookyContext
                           )(
                             rowFn: SquashedFetchedRow => SquashedFetchedRow
                             //apply immediately after depth selection, this include depth0
                             //should include flatten & extract
                           ): Unit = {

    import impl._
    import params._

    implicit def withSchema(row: SquashedFetchedRow): SquashedFetchedRow#WithSchema = new row.WithSchema(schema)

    val bestOpen: (TraceView, Iterable[DataRow]) = open.min(pairOrdering) //TODO: expensive! use pre-sorted collection

    open -= bestOpen._1

    val existingVisitedOption = ExploreRunnerCache.get(bestOpen._1 -> executionID, visitedReducer)

    val bestOpenAfterElimination: (TraceView, Iterable[DataRow]) = existingVisitedOption match {
      case Some(allVisited) =>
        val dataRowsAfterElimination = eliminator(bestOpen._2, allVisited)
        bestOpen.copy(_2 = dataRowsAfterElimination)
      case None =>
        bestOpen
    }

    if (bestOpenAfterElimination._2.nonEmpty) {
      val bestRow_- = SquashedFetchedRow(bestOpen._2.toArray, bestOpen._1)

      val bestRow = rowFn.apply(
        bestRow_-
          .extract(depth_++)
      )
      val bestDataRowsInRange = bestRow.dataRows.filter {
        dataRow =>
          range.contains(dataRow.getInt(depth_++.field).get)
      }

      this.commitIntoVisited(bestOpen._1, bestDataRowsInRange, visitedReducer)

      val bestNonFringeRow = bestRow.copy(
        dataRows = bestRow.dataRows.filter{
          dataRow =>
            dataRow.getInt(depth_++.field).get < range.max
        }
      )

      val opens_+ : Array[(TraceView, DataRow)] = bestNonFringeRow
        .extract(resolved)
        .flattenData(resolved.field, ordinalField, joinType.isLeft, sampler)
        .interpolate(trace)
        .map {
          tuple =>
            tuple._1 -> tuple._2
        }
      opens_+.foreach {
        open_+ =>
          val TraceView_+ = open_+._1
          val oldDataRows: Iterable[DataRow] = open.getOrElse(TraceView_+, Nil)
          val newDataRows = openReducer(Array(open_+._2), oldDataRows).toArray
          open += TraceView_+ -> newDataRows
      }
    }
  }

  def execute(
               resolved: Resolved[Any],
               sampler: Sampler[Any],
               joinType: JoinType,

               traces: Set[Trace]
             )(
               maxItr: Int,
               impl: ExploreImpl,
               `depth_++`: Resolved[Int],
               spooky: SpookyContext
             )(
               rowFn: SquashedFetchedRow => SquashedFetchedRow
               //apply immediately after depth selection, this include depth0
               //should include flatten & extract
             ): Iterator[(TraceView, Open_Visited)] = {

    // export openSet and visitedSet: they DO NOT need to be cogrouped: Spark shuffle will do it anyway.
    // a big problem here is whether each weakly referenced DataRow in the cache can be exported multiple times. Does this mess with the reducer?
    def finish(): Iterator[(TraceView, Open_Visited)] = {
      val open = this.open
        .map(t => t._1 -> Open_Visited(open = Some(t._2.toArray)))
      val visited = this.visited
        .map(t => t._1 -> Open_Visited(visited = Some(t._2.toArray)))

      val toBeCommitted = this.visited
        .map {
          tuple =>
            (tuple._1 -> executionID) -> tuple._2
        }

      ExploreRunnerCache.commit(toBeCommitted, impl.visitedReducer)

      this.finalize()

      (open ++ visited).iterator
    }

    for (i <- 0 to maxItr) {
      if (open.isEmpty) return finish()
      executeOnce(resolved, sampler, joinType, traces)(impl, `depth_++`, spooky)(rowFn)
    }

    finish()
  }

  override def finalize(): Unit = {
    ExploreRunnerCache.deregister(this)
  }
}