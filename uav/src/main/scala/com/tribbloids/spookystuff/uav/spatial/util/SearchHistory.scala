package com.tribbloids.spookystuff.uav.spatial.util

import com.tribbloids.spookystuff.uav.spatial.point.CoordinateSystem

import scala.collection.mutable

/**
  * Created by peng on 26/02/17.
  */
case class SearchHistory(
                          stack: mutable.ArrayBuffer[SearchAttempt[_]] = mutable.ArrayBuffer.empty,
                          failed: mutable.Set[SearchAttempt[_]] = mutable.Set.empty,
                          var hops: Int = 0,
                          var recursions: Int = 0
                        ) {

  override def toString: String = {
    s"hops=$hops recursions=$recursions"
  }

  def isBlacklisted(triplet: SearchAttempt[_]) = {
    stack.contains(triplet) || failed.contains(triplet)
  }

  def isAllowed(triplet: SearchAttempt[_]) = !isBlacklisted(triplet)

  def getCoordinate(
                     attempt: SearchAttempt[CoordinateSystem]
                   ): Option[attempt.system.Coordinate] = {

    recursions += 1

    if (isBlacklisted(attempt))
      return None

    //    import triplet._
    assert(stack.count(_ == attempt) == 0)
    stack += attempt
    if (hops <= stack.size) hops = stack.size
    try {
      val result = attempt.to._getCoordinate(attempt.system, attempt.from, this)
      result match {
        case None =>
          failed += attempt
        case _ =>
      }
      result
    }
    finally {
      assert(stack.last == attempt)
      assert(stack.count(_ == attempt) == 1)
      stack -= attempt
    }
  }
}
