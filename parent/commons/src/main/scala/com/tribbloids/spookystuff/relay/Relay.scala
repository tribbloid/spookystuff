package com.tribbloids.spookystuff.relay

import com.tribbloids.spookystuff.relay.io.{DateSerializer, Decoder, Encoder, FormattedText}
import com.tribbloids.spookystuff.relay.xml.{XMLFormats, Xml}
import org.apache.spark.ml.util.Identifiable
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.jackson.JsonMethods
import org.json4s.{Formats, JField, JValue, StringInput}

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
abstract class Relay[Proto] {
  // TODO: should be `BiRelay`

  import Relay.*

  type IR_>> <: IR
  type IR_<< <: IR

  def fallbackFormats: Formats = Relay.defaultFormats

  implicit def findRelay: Relay[Proto] = this
  implicit def toEncoder_>>(v: Proto): Encoder[IR_>>] = {

    val ir = toMessage_>>(v)
    Encoder[IR_>>](
      ir,
      this.fallbackFormats
    )
  }

  implicit class ProtoOps(v: Proto) {

    lazy val ir: IR_>> = toMessage_>>(v)

    def treeText: String = {

      val writer = FormattedText.NoColor_2.TreeWriter(ir)
      writer.text
    }

    def pathText_\ : String = {

      val writer = FormattedText.Path_\\\.TreeWriter(ir)
      writer.text
    }

    def pathText_/ : String = {

      val writer = FormattedText.Path_/:/.TreeWriter(ir)
      writer.text
    }
  }

  def toMessage_>>(v: Proto): IR_>>
  def toMessageBody(v: Proto): IR_>> #Body = toMessage_>>(v).body

  def toProto_<<(v: IR_<<): Proto

  case class DecoderView(
      decoder: Decoder[IR_<<]
  ) {

    def _outer: Relay[Proto] = Relay.this

    def jFieldToProto(jf: JField): Proto = {

      val ir = decoder.apply(jf)
      toProto_<<(ir)
    }

    final def fromJField(jf: JField): Proto = {
      val proto = jFieldToProto(jf)
      proto
    }

    final def fromJValue(jv: JValue): Proto = {
      val rootTag = decoder.defaultRootTag
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
    ): MessageMLParam[Proto] = new MessageMLParam[Proto](this, parent, name, doc, isValid)

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

  implicit def fallbackRelay[T: Manifest]: Relay[T] = new ToSelf[T]()

  // TODO: if chain summoning is stable, should only define `asDecoder`
  implicit def toFallbackDecoderView[R <: Relay[?]](relay: R)(
      implicit
      <:< : TreeIR.Leaf[relay.IR_<< #Body] <:< relay.IR_<<,
      typeInfo: Manifest[relay.IR_<< #Body]
  ): relay.DecoderView = {
    val decoder: Decoder[TreeIR.Leaf[relay.IR_<< #Body]] =
      Decoder.Plain[relay.IR_<< #Body](relay.fallbackFormats)(typeInfo)
    relay.DecoderView(decoder.asInstanceOf[Decoder[relay.IR_<<]])
  }

  final def xmlStr2Node(xml: String): Elem = {
    val bomRemoved = xml.replaceAll("[^\\x20-\\x7e]", "").trim // remove BOM (byte order mark)
    val prologRemoved = bomRemoved.replaceFirst("[^<]*(?=<)", "")
    val result = XML.loadString(prologRemoved)
    result
  }

  trait >>[Proto <: ProtoAPI] extends Relay[Proto] {

    final override def toMessage_>>(vv: Proto): IR_>> = {

      vv.toMessage_>>.asInstanceOf[IR_>>]
    }
  }

  trait Symmetric[Proto] extends Relay[Proto] {

    final override type IR_<< = IR_>>
  }

  trait ToMsg[Proto] extends Symmetric[Proto] {

    type Msg
    final override type IR_>> = IR.Aux[Msg]
  }

  object ToMsg {}

  trait <<[Proto] extends ToMsg[Proto] {

    override type Msg <: MessageAPI.<<

    final override def toProto_<<(vv: IR.Aux[IR_>> #Body]): Proto = {

      vv.body.toProto_<<.asInstanceOf[Proto]
    }
  }

  class ToSelf[Proto] extends ToMsg[Proto] {

    final type Msg = Proto

    override def toMessage_>>(v: Proto): TreeIR.Leaf[Proto] = TreeIR.leaf(v)

    override def toProto_<<(v: IR.Aux[Proto]): Proto = v.body
  }

  trait ToSelf_Imp0 {

    implicit def fallback[T]: ToSelf[T] = new ToSelf[T]()
  }

  object ToSelf extends ToSelf[Any] with ToSelf_Imp0

  object JValueRelay extends ToSelf[JValue] with ToSelf_Imp0
}
