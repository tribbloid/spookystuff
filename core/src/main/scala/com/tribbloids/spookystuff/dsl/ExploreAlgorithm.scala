package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.execution.{ExploreAlgorithmImpl, ShortestPathImpl}
import com.tribbloids.spookystuff.expressions.Expression
import com.tribbloids.spookystuff.row._

/**
  * Created by peng on 1/27/15.
  */
sealed trait ExploreAlgorithm {

  def getImpl(
               depthField: Field,
               ordinalField: Field,
               extracts: Seq[Expression[Any]]
             ): ExploreAlgorithmImpl
}
object ExploreAlgorithms {
  case object ShortestPath extends ExploreAlgorithm {

    override def getImpl(
                          depthField: Field,
                          ordinalField: Field,
                          extracts: Seq[Expression[Any]]
                        ): ExploreAlgorithmImpl = ShortestPathImpl(depthField, ordinalField, extracts)
  }
}

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