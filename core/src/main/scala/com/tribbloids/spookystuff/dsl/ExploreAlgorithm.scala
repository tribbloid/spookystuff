package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.ExploreAlgorithms.ExploreImpl
import com.tribbloids.spookystuff.execution.ExploreParams
import com.tribbloids.spookystuff.row._

sealed trait ExploreAlgorithm {

  def getImpl(
               params: ExploreParams,
               schema: DataRowSchema
             ): ExploreImpl
}

object ExploreAlgorithms {

  trait ExploreImpl {

    val params: ExploreParams
    val schema: DataRowSchema

    /**
      *
      */
    val openReducer: RowReducer

    def openReducerBetweenBatches: RowReducer = openReducer

    /**
      *
      */
    val visitedReducer: RowReducer //precede eliminator

    def visitedReducerBetweenBatches: RowReducer = visitedReducer

    /**
      *
      */
    val ordering: RowOrdering

    final lazy val pairOrdering = ordering.on {
      v: (TraceView, Iterable[DataRow]) => v._2
    }

    /**
      *
      */
    val eliminator: RowEliminator
  }

  case object ShortestPath extends ExploreAlgorithm {

    override def getImpl(
                          params: ExploreParams,
                          schema: DataRowSchema
                        ) = Impl(params, schema)

    case class Impl(
                     override val params: ExploreParams,
                     schema: DataRowSchema
                   ) extends ExploreImpl {

      import params._

      import scala.Ordering.Implicits._

      override val openReducer: RowReducer = {
        (v1, v2) =>
          (v1 ++ v2)
            .groupBy(_.groupID)
            .values
            .minBy(_.head.sortIndex(Seq(depthField, ordinalField)))
      }

      override val visitedReducer: RowReducer = {
        (v1, v2) =>
          (v1 ++ v2)
            .groupBy(_.groupID)
            .values
            .minBy(_.head.sortIndex(Seq(depthField, ordinalField)))
      }

      override val ordering: RowOrdering = Ordering.by {
        v: Iterable[DataRow] =>
          assert(v.size == 1)
          v.head.getInt(depthField)
      }

      override val eliminator: RowEliminator = {
        (v1, v2) =>
          val visitedDepth = v2.head.getInt(depthField)
          v1.filter {
            row =>
              row.getInt(depthField) < visitedDepth
          }
      }
    }
  }

  //move reduce of openSet to elimination, should have identical result
  //case class ShortestPathImpl2(
  //                              depthField: IndexedField,
  //                              ordinalField: IndexedField,
  //                              extracts: Seq[Expression[Any]]
  //                            ) extends ExploreAlgorithmImpl {
  //
  //  import scala.Ordering.Implicits._
  //
  //  override def openReducer: RowReducer = {
  //    _ ++ _
  //  }
  //
  //  override def visitedReducer: RowReducer = {
  //    (v1, v2) =>
  //      Array((v1 ++ v2).minBy(_.sortIndex(Seq(depthField, ordinalField))))
  //  }
  //
  //  override def ordering: RowOrdering = Ordering.by{
  //    v: Iterable[DataRow] =>
  //      assert(v.size == 1)
  //      v.head.getInt(depthField).get
  //  }
  //
  //  override def eliminator: RowEliminator = {
  //    (v1, v2) =>
  //      assert(v2.size == 1)
  //      val visitedDepth = v2.head.getInt(depthField).get
  //      val filtered = v1.filter {
  //        row =>
  //          row.getInt(depthField).get < visitedDepth
  //      }
  //      if (filtered.isEmpty) filtered
  //      else Some(filtered.minBy(_.sortIndex(Seq(depthField, ordinalField))))
  //  }
  //}
}

//TODO: finish these
//case object AllSimplePath extends ExploreOptimizer

//case object AllPath extends ExploreOptimizer {
//  override def openReducer: RowReducer = {_ ++ _}
//
//  //precede eliminator
//  override def ordering: RowOrdering = {_ ++ _}
//
//  override def visitedReducer: RowReducer = {_ ++ _}
//
//  override def eliminator: RowEliminator = {(v1, v2) => v1}
//}

//this won't merge identical traces and do lookup, only used in case each resolve may yield different result
//case class Dijkstra(
//
//                   ) extends ExploreOptimizer {
//  override def openReducer: RowReducer = ???
//
//  //precede eliminator
//  override def ordering: RowOrdering = ???
//
//  override def visitedReducer: RowReducer = ???
//
//  override def eliminator: RowEliminator = ???
//}