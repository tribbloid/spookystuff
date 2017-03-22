package com.tribbloids.spookystuff.uav.spatial

import scala.collection.mutable

/**
  * Created by peng on 26/02/17.
  */

case class PendingTriplet[+T <: CoordinateSystem](
                                                  from: Anchor,
                                                  system: T,
                                                  to: Anchor
                                                ) {
}

case class InferenceContext(
                             stack: mutable.ArrayBuffer[PendingTriplet[CoordinateSystem]] = mutable.ArrayBuffer.empty,
                             failed: mutable.Set[PendingTriplet[CoordinateSystem]] = mutable.Set.empty,
                             var hops: Int = 0,
                             var recursions: Int = 0
                           ) {

  override def toString: String = {
    s"hops=$hops recursions=$recursions"
  }

  def isForbidden(triplet: PendingTriplet[CoordinateSystem]) = {
    stack.contains(triplet) || failed.contains(triplet)
  }

  def isAllowed(triplet: PendingTriplet[CoordinateSystem]) = !isForbidden(triplet)

  def getCoordinate(
                     triplet: PendingTriplet[CoordinateSystem]
                   ): Option[triplet.system.V] = {

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
