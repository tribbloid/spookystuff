package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.row.{DataRow, Field, _}

abstract class ExploreAlgorithmImpl {

  def depthField: Field
  def ordinalField: Field

  def range = depthField.depthRangeOpt.get

  def openReducer: RowReducer
  def visitedReducer: RowReducer //precede eliminator
  def ordering: RowOrdering
  def eliminator: RowEliminator

  //TODO: this should contains any chain of extract, flatten, or map-ish execution plan
  def extracts: Seq[NamedExtr[Any]]
}

case class ShortestPathImpl(
                             depthField: Field,
                             ordinalField: Field,
                             extracts: Seq[NamedExtr[Any]]
                           ) extends ExploreAlgorithmImpl {

  import scala.Ordering.Implicits._

  override def openReducer: RowReducer = {
    (v1, v2) =>
      (v1 ++ v2)
        .groupBy(_.groupID)
        .values
        .minBy(_.head.sortIndex(Seq(depthField, ordinalField)))
  }

  override def visitedReducer: RowReducer = {
    (v1, v2) =>
      (v1 ++ v2)
        .groupBy(_.groupID)
        .values
        .minBy(_.head.sortIndex(Seq(depthField, ordinalField)))
  }

  override def ordering: RowOrdering = Ordering.by {
    v: Iterable[DataRow] =>
      assert(v.size == 1)
      v.head.getInt(depthField).get
  }

  override def eliminator: RowEliminator = {
    (v1, v2) =>
      val visitedDepth = v2.head.getInt(depthField).get
      v1.filter {
        row =>
          row.getInt(depthField).get < visitedDepth
      }
  }
}

//move reduce of openSet to elimination, should have identical result
//case class ShortestPathImpl2(
//                              depthField: Field,
//                              ordinalField: Field,
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