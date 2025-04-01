package com.tribbloids.spookystuff.extractors

import ai.acyclic.prover.commons.debug.CallStackRef

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.runtime.AbstractPartialFunction

// Result of PartialFunctions.unlift in scala 2.11 is NOT serializable, but this problem is long gone
// only kept here to memorize construction stacktrace
case class Unlift[-T, +R](
    liftFn: T => Option[R]
) extends AbstractPartialFunction[T, R] {
  // TODO: this should be moved into prover-commons function

  val id: String = UUID.randomUUID().toString
  Unlift.id2ConstructionStack += id -> CallStackRef.here.stack

  final override def isDefinedAt(x: T): Boolean = liftFn(x).isDefined

  final override def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = {
    val z = liftFn(x)
    z.getOrElse(default(x))
  }

  final override def lift: Function1[T, Option[R]] = liftFn
}

object Unlift {

  def id2ConstructionStack: TrieMap[String, Seq[StackTraceElement]] = TrieMap.empty
}
