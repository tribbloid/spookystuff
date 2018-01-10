package org.apache.spark.ml.dsl.utils.metadata

import com.tribbloids.spookystuff.utils.TreeException
import org.apache.spark.ml.dsl.utils.messaging.{MessageRelay, MessageWriter, Nested, Registry}
import org.json4s
import org.json4s.JsonAST.{JObject, JString, JValue}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

trait MetadataKey extends Serializable {
  val name: String
}
object MetadataKey {


}

object MetadataMap {

  def apply(vs: Tuple2[MetadataKey, Any]*) = ListMap(vs.map{case (k,v) => k.name -> v}: _*)
}


case class Metadata(
                     map: ListMap[String, Any] = ListMap.empty
                   ) {

  //  def +(tuple: (Param[_], Any)) = this.copy(this.map + (tuple._1.key -> tuple._2))
  //  def ++(v2: Metadata) = this.copy(this.map ++ v2.map)

  case class Param[T](name: String) extends MetadataKey {

    def apply(): T = map(name).asInstanceOf[T]

    def get: Option[T] = map.get(name).map(_.asInstanceOf[T])
  }

  //WARNING: DO NOT add implicit conversion to inner class' companion object! will trigger "java.lang.AssertionError: assertion failed: mkAttributedQualifier(_xxx ..." compiler error!
  //TODO: report bug to scala team!
  //  object Param {
  //    implicit def toStr(v: Param[_]): String = v.k
  //
  //    implicit def toKV[T](v: (Param[_], T)): (String, T) = v._1.k -> v._2
  //  }
}

object Metadata extends MessageRelay[Metadata] {

  object empty extends Metadata()

  override def getRootTag(protoOpt: Option[Metadata], messageOpt: Option[Map[String, JValue]]): String = "root"

  private val jvBlacklist: Set[JValue] = Set(
    JObject()
  )
  def assertWellFormed(jv: JValue): JValue = {
    assert(!jvBlacklist.contains(jv))
    jv
  }

  implicit def fromStrMap(map: Map[String, Any]) = apply(map.toSeq: _*)

  def apply(vs: Tuple2[String, Any]*): Metadata = Metadata(ListMap(vs: _*))

  type M = Map[String, JValue]
  override def messageMF = implicitly[Manifest[M]]

  override def toMessage_>>(md: Metadata): M = {
    val result: Seq[(String, json4s.JValue)] = md.map
      .toSeq
      .map {
        case (k, v) =>
          val mapped = Nested[Any](v).map[JValue] {
            elem: Any =>
              TreeException.|||^[JValue](Seq(
                {
                  () =>
                    val codec = Registry.Default.findCodecOrDefault(v)
                    assertWellFormed(codec.toWriter_>>(elem).toJValue)
                },
                {
                  () =>
                    JString(elem.toString)
                }
              ))
                .get
          }
          k -> MessageWriter(mapped.self).toJValue
      }

    ListMap(result: _*)
  }

  override def toProto_<<(m: M, rootTag: String): Metadata = {
    val map = m
      .toSeq
      .map {
        case (k, jv) =>
          val mapped = Nested[Any](jv).map(
            fn = identity,
            preproc = {
              case v: JValue => v.values
              case v@ _ => v
            })
          k -> mapped.self
        //          RecursiveTransform(jv, failFast = true)(
        //            {
        //              case v: JValue => v.values
        //              case v@ _ => v
        //            }
        //          )
      }
    apply(map: _*)
  }
}
