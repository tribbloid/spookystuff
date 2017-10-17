package com.tribbloids.spookystuff.uav.spatial.util

import com.tribbloids.spookystuff.uav.spatial.SpatialSystem

import scala.collection.mutable

/**
  * Created by peng on 26/02/17.
  */
case class SearchHistory(
                          stack: mutable.ArrayBuffer[SearchAttempt[SpatialSystem]] = mutable.ArrayBuffer.empty,
                          failed: mutable.Set[SearchAttempt[SpatialSystem]] = mutable.Set.empty,
                          var hops: Int = 0,
                          var recursions: Int = 0
                        ) {

  override def toString: String = {
    s"hops=$hops recursions=$recursions"
  }

  def isForbidden(triplet: SearchAttempt[SpatialSystem]) = {
    stack.contains(triplet) || failed.contains(triplet)
  }

  def isAllowed(triplet: SearchAttempt[SpatialSystem]) = !isForbidden(triplet)

  def getCoordinate(
                     triplet: SearchAttempt[SpatialSystem]
                   ): Option[triplet.system.C] = {

    recursions += 1

    if (isForbidden(triplet))
      return None

    //    import triplet._
    assert(stack.count(_ == triplet) == 0)
    stack += triplet
    if (hops <= stack.size) hops = stack.size
    try {
      val result = triplet.to._getCoordinate(triplet.system, triplet.from, this)
      result match {
        case None =>
          failed += triplet
        case _ =>
      }
      result
    }
    finally {
      assert(stack.last == triplet)
      assert(stack.count(_ == triplet) == 1)
      stack -= triplet
    }
  }
}
