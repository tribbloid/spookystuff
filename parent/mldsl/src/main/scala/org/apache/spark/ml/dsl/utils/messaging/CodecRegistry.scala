package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ScalaType

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * add for runtime lookup of Relay/Reader in case implicit information for generic programming was erased.
  */
trait CodecRegistry {

  // failed lookup won't be reattempted as it is still too slow.
  val registry: mutable.Map[ScalaType[_], Try[Codec[_]]] = mutable.Map.empty

  /**
    * this is a bunch of makeshift rules that allow type class pattern to be used in runtime
    *
    * if extending MessageCodec.API, use it
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
  def tryFindCodecFor[T](v: T): Try[Codec[T]] = {
    val result: Try[Codec[_]] = v match {
      case v: Codec[_]#API =>
        Success(v.outer)
      case _ =>
        val clazz = v.getClass
        val scalaType: ScalaType[_] = clazz

        registry.getOrElseUpdate(
          scalaType, {
            val companions = scalaType.utils.baseCompanionObjects
            val codecOpt = companions.find(_.isInstanceOf[Codec[_]])
            codecOpt match {
              case Some(codec: Codec[_]) =>
                Success(codec)
              case _ =>
                Failure(new UnsupportedOperationException(s"$clazz has no companion Codec"))
            }
          }
        )
    }
    result.map(_.asInstanceOf[Codec[T]])
  }

  def findCodecFor[T](v: T): Codec[T] = tryFindCodecFor(v).get

  def findCodecOrDefault[T: Manifest](v: T): Codec[T] = tryFindCodecFor(v).getOrElse(
    new MessageReader[T]
  )
}

object CodecRegistry {

  /**
    * the registry is empty at the beginning.
    */
  object Default extends CodecRegistry {}

//  /**
//    * the registry is populated by every codec on object creation
//    */
  //  object AllInclusive extends Catalog {
  //
  //  }
}
