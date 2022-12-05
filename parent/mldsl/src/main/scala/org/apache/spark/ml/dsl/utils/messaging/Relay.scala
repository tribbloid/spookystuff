package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.messaging.xml.{XMLFormats, Xml}
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.apache.spark.ml.util.Identifiable
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.jackson.JsonMethods
import org.json4s.{Extraction, Formats, JField, JValue}

import scala.language.implicitConversions
import scala.xml.{Elem, NodeSeq, XML}

/**
  * mixin to allow converting to a message object and back used to delegate ser/de tasks (from/to xml, json, Dataset
  * encoding, protobuf) to the case class with a fixed schema all concreate subclasses must be singletons.
  *
  * Where to find implicit conversions?
  *
  * First look in current scope
  *
  * Implicits defined in current scope Explicit imports wildcard imports
  *
  * Now look at associated types in
  *
  * Companion objects of a type \- include companion objects of an object's self type, all supertypes, all parameter
  * types, all parameter types' supertypes Implicit scope of an argument’s type (2.9.1) - e.g. Companion objects
  * Implicit scope of type arguments (2.8.0) - e.g. Companion objects Outer objects for nested types
  */
abstract class Relay[Proto] extends RelayLevel1 with RootTagged {

  import Relay._

  def fallbackFormats: Formats = Relay.defaultFormats

  implicit def findCodec: Relay[Proto] = this
  implicit def toEncoder_>>(v: Proto): Encoder[Msg] = {

    val msg = toMessage_>>(v)
    val rootTagOvrd = Relay.RootTagOf(v, msg, this).default
    Encoder[Msg](
      msg,
      this.fallbackFormats,
      Some(rootTagOvrd)
    )
  }

  def toMessage_>>(v: Proto): Msg
  def toProto_<<(v: Msg, rootTag: String): Proto

  case class Decoder(formats: Formats = fallbackFormats)(
      implicit
      val messageMF: Manifest[Msg]
  ) {

    def _outer: Relay[Proto] = Relay.this

    def jFieldToProto(jf: JField): Proto = {

      val mf = this.messageMF
      val m = Extraction.extract[Msg](jf._2)(formats, mf)
      toProto_<<(m, jf._1)
    }

    final def fromJField(jf: JField): Proto = {
      val proto = jFieldToProto(jf)
      proto
    }

    final def fromJValue(jv: JValue): Proto = {
      val rootTag = _outer.rootTag
      fromJField(rootTag -> jv)
    }

    final def fromJSON(json: String): Proto = fromJValue(JsonMethods.parse(json))

    final def fromXMLNode(ns: NodeSeq): Proto = {
      val jv: JValue = Xml.toJson(ns)
      jv match {
        case JObject(kvs) =>
          fromJField(kvs.head)
        case JArray(vs) =>
          fromJValue(vs.head)
        case _ =>
          fromJValue(jv) // TODO: not possible!
      }
    }

    final def fromXML(xml: String): Proto = {
      val nodes: Elem = xmlStr2Node(xml)

      fromXMLNode(nodes)
    }

    def Param(
        parent: String,
        name: String,
        doc: String,
        isValid: Proto => Boolean
    ) = new MessageMLParam[Proto](this, parent, name, doc, isValid)

    def Param(parent: String, name: String, doc: String): MessageMLParam[Proto] =
      Param(parent, name, doc, (_: Proto) => true)

    def Param(parent: Identifiable, name: String, doc: String, isValid: Proto => Boolean): MessageMLParam[Proto] =
      Param(parent.uid, name, doc, isValid)

    def Param(parent: Identifiable, name: String, doc: String): MessageMLParam[Proto] =
      Param(parent.uid, name, doc)
  }

  trait API {

    def outer: Relay[Proto] = Relay.this
  }
}

object Relay {

  lazy val defaultFormats: Formats = XMLFormats.defaultFormats + DateSerializer

  case class RootTagOf(chain: Any*) {

    val first: Any = chain.head // sanity check

    lazy val explicitOpt: Option[String] = {

      val trials = chain.toStream.map {
        case vv: RootTagged =>
          Some(vv.rootTag)

        case vv: Product =>
          Some(vv.productPrefix)
        case _ =>
          None
      }

      trials.collectFirst {
        case Some(v) => v
      }
    }

    lazy val fallback: String = first match {

//      case vv: GenTraversableLike[_, _] =>
//        vv.stringPrefix
      case _ =>
        ScalaType.getRuntimeType(first).asClass.getSimpleName.stripSuffix("$")
    }

    lazy val default: String = explicitOpt.getOrElse(fallback)
  }

  implicit def fallbackRelay[T: Manifest]: Relay[T] = new MessageReader[T]()

  implicit def asDecoder[Proto](v: Relay[Proto])(
      implicit
      messageMF: Manifest[v.Msg]
  ): v.Decoder = v.Decoder(v.fallbackFormats)(messageMF)

  final def xmlStr2Node(xml: String): Elem = {
    val bomRemoved = xml.replaceAll("[^\\x20-\\x7e]", "").trim // remove BOM (byte order mark)
    val prologRemoved = bomRemoved.replaceFirst("[^<]*(?=<)", "")
    val result = XML.loadString(prologRemoved)
    result
  }

  trait >>[Proto <: ProtoAPI] extends Relay[Proto] {

    final override def toMessage_>>(vv: Proto): Msg = {

      vv.toMessage_>>.asInstanceOf[Msg]
    }
  }

  trait <<[Proto] extends Relay[Proto] {

    override type Msg <: MessageAPI.<<

    final override def toProto_<<(vv: Msg, rootTag: String): Proto = {

      vv.toProto_<<.asInstanceOf[Proto]
    }
  }
}
