package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.Xml
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.apache.spark.ml.util.Identifiable
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{Extraction, Formats, JField, JValue}
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.xml.{Elem, NodeSeq, XML}

trait CodecLevel1 {

  def formats: Formats = Codec.defaultFormats

  type M //Message type
  protected implicit def messageMF: Manifest[M]

  lazy val defaultRootTag = this.getClass.getSimpleName.stripSuffix("$")

  def rootTag: String = defaultRootTag

  //  implicit def proto2Message(m: M): MessageWriter[M] = {
  //    MessageWriter[M](m, this.formats)
  //  }
}

object Codec {

  def defaultFormats: Formats = Xml.defaultFormats
}

/**
  * mixin to allow converting to a message object and back
  * used to delegate ser/de tasks (from/to xml, json, Dataset encoding, protobuf) to the case class with a fixed schema
  * all concreate subclasses must be singletons.
  *
  * Where to find implicit conversions?
  *
First look in current scope

Implicits defined in current scope
Explicit imports
wildcard imports

Now look at associated types in

Companion objects of a type
  - include companion objects of an object's self type, all supertypes, all parameter types, all parameter types' supertypes
Implicit scope of an argument’s type (2.9.1) - e.g. Companion objects
Implicit scope of type arguments (2.8.0) - e.g. Companion objects
Outer objects for nested types

  * @tparam Self
  */
abstract class Codec[Self] extends CodecLevel1 {

  implicit def findCodec: Codec[Self] = this
  implicit def toWriter_>>(v: Self): MessageWriter[M] = MessageWriter[M](toMessage_>>(v), this.formats, Some(this.rootTag))

  def selfType: ScalaType[Self]

//  Catalog.AllInclusive.registry += selfType -> this

  def toMessage_>>(v: Self): M
  def toSelf_<<(v: M): Self

  def fromJField(jf: JField, formats: Formats = this.formats): Self = {

    val mf = this.messageMF
    val m = Extraction.extract[M](jf._2)(formats, mf)
    toSelf_<<(m)
  }

  final def _fromJField[T: Codec](jf: JField): T = {
    val reader = implicitly[Codec[T]]
    reader.fromJField(jf, this.formats)
  }
  final def _fromJValue[T: Codec](jv: JValue): T = {
    val reader = implicitly[Codec[T]]
    val rootTag = reader.rootTag
    _fromJField(rootTag -> jv)(reader)
  }
  final def _fromJSON[T: Codec](json: String): T = _fromJValue[T](parse(json))

  final def _fromXMLNode[T: Codec](ns: NodeSeq): T = {
    val jv: JValue = Xml.toJson(ns)
    jv match {
      case JObject(kvs) =>
        _fromJField[T](kvs.head)
      case JArray(vs) =>
        _fromJValue[T](vs.head)
      case _ =>
        _fromJValue[T](jv) //TODO: not possible!
    }
  }
  final def _fromXML[T: Codec](xml: String): T = {
    val nodes: Elem = xmlStr2Node(xml)

    _fromXMLNode[T](nodes)
  }
  final def _toXMLAndBack[T: Codec](proto: T): T = {
    val xml = MessageWriter(proto).prettyXML
    LoggerFactory.getLogger(this.getClass).info(
      s"""
         |========================= XML ========================
         |$xml
         |======================== /XML ========================
      """.stripMargin
    )
    val back = this._fromXML[T](xml)
    back
  }

  final def xmlStr2Node(xml: String): Elem = {
    val bomRemoved = xml.replaceAll("[^\\x20-\\x7e]", "").trim //remove BOM (byte order mark)
    val prologRemoved = bomRemoved.replaceFirst("[^<]*(?=<)", "")
    val result = XML.loadString(prologRemoved)
    result
  }

  final def fromJValue(jv: JValue): Self = _fromJValue[Self](jv)(this)
  final def fromJSON(json: String): Self = _fromJSON[Self](json)(this)

  final def fromXMLNode(ns: NodeSeq): Self = _fromXMLNode[Self](ns)(this)
  final def fromXML(xml: String): Self = _fromXML[Self](xml)(this)

  //  final def toMessageAPIIfNot(v: Self): MessageAPI = {
  //    v match {
  //      case v: MessageAPI => v
  //      case _ => toMessageAPI(v)
  //    }
  //  }

  //  implicit class RelayView(self: Self) extends MessageAPI {
  //
  //    override def formats = RelayLike.this.formats
  //
  //    final def toM = RelayLike.this.toM(self)
  //    final def toMessageAPI = RelayLike.this.toMessageAPI(self)
  //    final def toMessageAPIIfNot = RelayLike.this.toMessageAPI(self)
  //
  //    override def proto: Any = toMessageAPI.proto
  //  }

  //  class UDT extends UserDefinedType[Obj] {
  //
  //    override def sqlType: DataType = ???
  //
  //    override def serialize(obj: Obj): Any = ???
  //
  //    override def deserialize(datum: Any): Obj = ???
  //
  //    override def userClass: Class[Obj] = ???
  //  }

  def Param(
             parent: String,
             name: String,
             doc: String,
             isValid: Self => Boolean,
             // serializer = SparkEnv.get.serializer
             formats: Formats = Codec.this.formats
           ): MessageMLParam[Self] = new MessageMLParam(this, parent, name, doc, isValid, formats)

  def Param(parent: String, name: String, doc: String): MessageMLParam[Self] =
    Param(parent, name, doc, (_: Self) => true)

  def Param(parent: Identifiable, name: String, doc: String, isValid: Self => Boolean): MessageMLParam[Self] =
    Param(parent.uid, name, doc, isValid)

  def Param(parent: Identifiable, name: String, doc: String): MessageMLParam[Self] =
    Param(parent.uid, name, doc)

  trait API {

    def outer: Codec[Self] = Codec.this
  }
}
