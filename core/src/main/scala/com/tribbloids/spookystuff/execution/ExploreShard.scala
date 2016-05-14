package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.caching.{ConcurrentMap, ExploreSharedVisitedCache}
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.NOTSerializableMixin
import com.tribbloids.spookystuff.{SpookyContext, dsl}

// use Array to minimize serialization footage
case class Open_Visited(
                         open: Option[Array[DataRow]] = None,
                         visited: Option[Array[DataRow]] = None
                       )

/**
  * NOT serializable: expected to be constructed on Executors
  */
class ExploreShard(
                        val itr: Iterator[(Trace, Open_Visited)],
                        val executionID: Long
                      ) extends NOTSerializableMixin {

  import dsl._

  import scala.collection.JavaConverters._

  val _open: ConcurrentMap[Trace, Iterable[DataRow]] = ConcurrentMap()
  def open = _open.asScala
  //  val openVs = mutable.SortedSet[(Trace, Array[DataRow])] = mutable.SortedSet() //TODO: add fast sort implementation
  val _visited: ConcurrentMap[Trace, Iterable[DataRow]] = ConcurrentMap()
  def visited = _visited.asScala

  itr.foreach{
    tuple =>
      tuple._2.open.map(v => open += tuple._1 -> v)
      tuple._2.visited.map(v => visited += tuple._1 -> v)
  }

  ExploreSharedVisitedCache.register(this)

  protected def commitIntoVisited(
                                   key: Trace,
                                   value: Iterable[DataRow],
                                   reducer: RowReducer
                                 ): Unit = {
    val oldVs: Iterable[DataRow] = visited.getOrElse(key, Nil)
    val newVs = reducer(value, oldVs)
    visited.put(key, newVs)
  }

  protected def executeOnce(
                             expr: NamedExtr[Any],
                             sampler: Sampler[Any],
                             joinType: JoinType,

                             traces: Set[Trace]
                           )(
                             depthKey: Field,
                             algorithmImpl: ExploreAlgorithmImpl,
                             spooky: SpookyContext
                           )(
                             rowFn: SquashedFetchedRow => SquashedFetchedRow
                             //apply immediately after depth selection, this include depth0
                             //should include flatten & extract
                           ): Unit = {

    import algorithmImpl._

    val pairOrdering = ordering.on {
      v: (Trace, Iterable[DataRow]) => v._2
    }
    val bestOpen: (Trace, Iterable[DataRow]) = open.min(pairOrdering) //TODO: expensive! pre-sort first

    open -= bestOpen._1

    val existingVisitedOption = ExploreSharedVisitedCache.get(bestOpen._1 -> executionID, visitedReducer)

    val bestOpenAfterElimination: (Trace, Iterable[DataRow]) = existingVisitedOption match {
      case Some(allVisited) =>
        val dataRowsAfterElimination = eliminator(bestOpen._2, allVisited)
        bestOpen.copy(_2 = dataRowsAfterElimination)
      case None =>
        bestOpen
    }

    if (bestOpenAfterElimination._2.nonEmpty) {
      val bestRow_- = SquashedFetchedRow(bestOpen._2.toArray, bestOpen._1).fetch(spooky)

      val bestRow = rowFn.apply(
        bestRow_-
          .extract(new GetExpr(depthKey).typed[Int].andThen(_ + 1) ~! depthKey)
      )
      val bestDataRowsInRange = bestRow.dataRows.filter {
        dataRow =>
          range.contains(dataRow.getInt(depthKey).get)
      }

      this.commitIntoVisited(bestOpen._1, bestDataRowsInRange, visitedReducer)

      val bestNonFringeRow = bestRow.copy(
        dataRows = bestRow.dataRows.filter{
          dataRow =>
            dataRow.getInt(depthKey).get < range.max
        }
      )

      val opens_+ : Array[(Trace, DataRow)] = bestNonFringeRow
        .extract(expr)
        .flattenData(expr.field, ordinalField, joinType.isLeft, sampler)
        .interpolate(traces, spooky)
      opens_+.foreach {
        open_+ =>
          val trace_+ = open_+._1
          val oldDataRows: Iterable[DataRow] = open.getOrElse(trace_+, Nil)
          val newDataRows = openReducer(Array(open_+._2), oldDataRows).toArray
          open += trace_+ -> newDataRows
      }
    }
  }

  def execute(
               expr: NamedExtr[Any],
               sampler: Sampler[Any],
               joinType: JoinType,

               traces: Set[Trace]
             )(
               maxItr: Int,
               algorithmImpl: ExploreAlgorithmImpl,
               spooky: SpookyContext
             )(
               rowFn: SquashedFetchedRow => SquashedFetchedRow
               //apply immediately after depth selection, this include depth0
               //should include flatten & extract
             ): Iterator[(Trace, Open_Visited)] = {

    // export openSet and visitedSet: they DO NOT need to be cogrouped: Spark shuffle will do it anyway.
    // a big problem here is whether each weakly referenced DataRow in the cache can be exported multiple times. Does this mess with the reducer?
    def finish(): Iterator[(Trace, Open_Visited)] = {
      val open = this.open
        .map(t => t._1 -> Open_Visited(open = Some(t._2.toArray)))
      val visited = this.visited
        .map(t => t._1 -> Open_Visited(visited = Some(t._2.toArray)))

      val toBeCommitted = this.visited
          .map {
            tuple =>
              (tuple._1 -> executionID) -> tuple._2
          }

      ExploreSharedVisitedCache.commit(toBeCommitted, algorithmImpl.visitedReducer)

      this.finalize()

      (open ++ visited).iterator
    }

    for (i <- 0 to maxItr) {
      if (open.isEmpty) return finish()
      executeOnce(expr, sampler, joinType, traces)(algorithmImpl.depthField, algorithmImpl, spooky)(rowFn)
    }

    finish()
  }

  override def finalize(): Unit = {
    ExploreSharedVisitedCache.deregister(this)
  }
}