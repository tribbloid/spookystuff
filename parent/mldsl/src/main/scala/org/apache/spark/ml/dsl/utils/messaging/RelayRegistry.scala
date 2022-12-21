package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ScalaType

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * add for runtime lookup of Relay/Reader in case implicit information for generic programming was erased.
  */
trait RelayRegistry {

  // failed lookup won't be reattempted as it is still too slow.
  val registry: mutable.Map[ScalaType[_], Try[Relay[_]]] = mutable.Map.empty

  /**
    * this is a bunch of makeshift rules that allow type class pattern to be used in runtime
    *
    * if extending Relay#API, use it
    *
    * otherwise use the corresponding codec if its already in the registry
    *
    * otherwise try to find it using the following list with descending precedence (and add into registry):
    *
    *   - if its companion object is a codec, use it (by convention: companion class == type class)
    *   - else if its super class' companion object is a codec, use it.
    *   - if multiple super classes/traits have codec companion objects, use the one closest in the inheritance graph
    *
    * @return
    */
  def tryLookupFor[T](v: T): Try[Relay[T]] = {
    val result: Try[Relay[_]] = v match {
      case v: Relay[_]#API =>
        Success(v.outer)
      case _ =>
        val clazz = v.getClass
        val scalaType: ScalaType[_] = clazz

        registry.getOrElseUpdate(
          scalaType, {
            val companions = scalaType.utils.baseCompanionObjects
            val rrOpt = companions.find(_.isInstanceOf[Relay[_]])
            rrOpt match {
              case Some(rr: Relay[_]) =>
                Success(rr)
              case _ =>
                Failure(new UnsupportedOperationException(s"$clazz has no Relay"))
            }
          }
        )
    }
    result.map(_.asInstanceOf[Relay[T]])
  }

  def lookupFor[T](v: T): Relay[T] = tryLookupFor(v).get

  def lookupOrDefault[T: Manifest](v: T): Relay[T] = tryLookupFor(v).getOrElse(
    new Relay.ToSelf[T]
  )
}

object RelayRegistry {

  /**
    * the registry is empty at the beginning.
    */
  object Default extends RelayRegistry {}

//  /**
//    * the registry is populated by every codec on object creation
//    */
  //  object AllInclusive extends Catalog {
  //
  //  }
}
