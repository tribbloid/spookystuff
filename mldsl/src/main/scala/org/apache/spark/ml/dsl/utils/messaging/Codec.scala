package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.messaging.xml.{XMLFormats, Xml}
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.apache.spark.ml.util.Identifiable
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.jackson.JsonMethods
import org.json4s.{Extraction, Formats, JField, JValue}
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.xml.{Elem, NodeSeq, XML}

trait CodecLevel1 {

  def formats: Formats = Codec.defaultFormats

  type M //Message type
  protected implicit def messageMF: Manifest[M]

  //  implicit def proto2Message(m: M): MessageWriter[M] = {
  //    MessageWriter[M](m, this.formats)
  //  }
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

  */
abstract class Codec[Proto] extends CodecLevel1 with HasRootTag {

  implicit def findCodec: Codec[Proto] = this
  implicit def toWriter_>>(v: Proto): MessageWriter[M] = {

    val msg = toMessage_>>(v)
    MessageWriter[M](
      msg,
      this.formats,
      Some(getRootTag(Some(v), Some(msg)))
    )
  }

  def getRootTag(protoOpt: Option[Proto], messageOpt: Option[M]): String = {
    messageOpt
      .map { v =>
        Codec.getRootTag(v)
      }
      .getOrElse(this.rootTag)
  }

  def selfType: ScalaType[Proto]

  //  Catalog.AllInclusive.registry += selfType -> this

  def toMessage_>>(v: Proto): M
  def toProto_<<(v: M, rootTag: String): Proto

  def fromJField(jf: JField, formats: Formats = this.formats): Proto = {

    val mf = this.messageMF
    val m = Extraction.extract[M](jf._2)(formats, mf)
    toProto_<<(m, jf._1)
  }

  final def _fromJField[T: Codec](jf: JField): T = {
    val reader = implicitly[Codec[T]]
    reader.fromJField(jf, this.formats)
  }
  final def _fromJValue[T: Codec](jv: JValue): T = {
    val reader = implicitly[Codec[T]]
    val rootTag = reader.getRootTag(None, None)
    _fromJField(rootTag -> jv)(reader)
  }
  final def _fromJSON[T: Codec](json: String): T = _fromJValue[T](JsonMethods.parse(json))

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
    val codec = implicitly[Codec[T]]
    val xml = codec.toWriter_>>(proto).prettyXML
    LoggerFactory
      .getLogger(this.getClass)
      .info(
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

  final def fromJValue(jv: JValue): Proto = _fromJValue[Proto](jv)(this)
  final def fromJSON(json: String): Proto = _fromJSON[Proto](json)(this)

  final def fromXMLNode(ns: NodeSeq): Proto = _fromXMLNode[Proto](ns)(this)
  final def fromXML(xml: String): Proto = _fromXML[Proto](xml)(this)

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
      isValid: Proto => Boolean,
      // serializer = SparkEnv.get.serializer
      formats: Formats = Codec.this.formats
  ): MessageMLParam[Proto] = new MessageMLParam(this, parent, name, doc, isValid, formats)

  def Param(parent: String, name: String, doc: String): MessageMLParam[Proto] =
    Param(parent, name, doc, (_: Proto) => true)

  def Param(parent: Identifiable, name: String, doc: String, isValid: Proto => Boolean): MessageMLParam[Proto] =
    Param(parent.uid, name, doc, isValid)

  def Param(parent: Identifiable, name: String, doc: String): MessageMLParam[Proto] =
    Param(parent.uid, name, doc)

  trait API {

    def outer: Codec[Proto] = Codec.this
  }
}

object Codec {

  def defaultFormats: Formats = XMLFormats.defaultFormats + DateSerializer

  def getRootTag(v: Any): String = {
    v match {
      case vv: HasRootTag =>
        vv.rootTag
      case _ =>
        getDefaultRootTag(v)
    }
  }

  def getDefaultRootTag(v: Any): String = {
    v match {
      case vv: Traversable[_] =>
        vv.stringPrefix
      case vv: Product =>
        vv.productPrefix
      case _ =>
        ScalaType.getRuntimeType(v).asClass.getSimpleName.stripSuffix("$")
    }
  }

  implicit def fallbackCodec[T: Manifest]: Codec[T] = new MessageReader[T]()
}
