package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{DataType, UserDefinedType}
import org.apache.spark.util.Utils
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.jackson.JsonMethods._
import org.json4s.{Extraction, Formats, JField, JValue}
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.util.Try
import scala.xml.{Elem, NodeSeq, XML}

//mixin to allow converting to a simple case class and back
//used to delegate ser/de tasks (from/to xml, json & dataset encoded type) to the case class with a fixed schema
//all subclasses must be objects otherwise Spark SQL can't find schema for Repr
abstract class RelayLike[Obj] {

  implicit def formats: Formats = Xml.defaultFormats

  implicit def mf: Manifest[M]

  type M //Message type

  def _read(jf: JField, formats: Formats, mf: Manifest[M]): M = {

    val message: M = Extraction.extract[M](jf._2)(formats, mf)
    message
  }

  final def _fromJField[T: MessageReader](jf: JField): T = {
    val reader = implicitly[MessageReader[T]]
    reader._read(jf, this.formats, reader.mf)
  }
  final def _fromJValue[T: MessageReader](jv: JValue): T = {
    val reader = implicitly[MessageReader[T]]
    val rootTag = reader.mf.runtimeClass.getSimpleName.stripSuffix("$")
    _fromJField(rootTag -> jv)
  }
  final def _fromJSON[T: MessageReader](json: String): T = _fromJValue[T](parse(json))

  final def _fromXMLNode[T: MessageReader](ns: NodeSeq): T = {
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
  final def _fromXML[T: MessageReader](xml: String): T = {
    val nodes: Elem = xmlStr2Node(xml)

    _fromXMLNode[T](nodes)
  }
  final def _toXMLAndBack[T: MessageReader](model: T): T = {
    val xml = MessageView(model).prettyXML
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

  implicit def reader: MessageReader[M]

  final def fromJValue(jv: JValue): M = _fromJValue[M](jv)
  final def fromJSON(json: String): M = _fromJSON[M](json)

  final def fromXMLNode(ns: NodeSeq): M = _fromXMLNode[M](ns)
  final def fromXML(xml: String): M = _fromXML[M](xml)

  def toM(v: Obj): M
  final def toMessageAPI(v: Obj): MessageAPI = {
    val m = toM(v)
    m match {
      case m: MessageAPI => m
      case _ => MessageView(m)
    }
  }
  final def toMessageAPIIfNot(v: Obj): MessageAPI = {
    v match {
      case v: MessageAPI => v
      case _ => toMessageAPI(v)
    }
  }

  trait API extends MessageAPI {
    self: Obj =>

    override def formats = RelayLike.this.formats

    final def toM = RelayLike.this.toM(self)
    final def toMessageAPI = RelayLike.this.toMessageAPI(self)
    final def toMessageAPIIfNot = RelayLike.this.toMessageAPI(self)

    override def proto: Any = toMessageAPI.proto
  }

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
             isValid: Obj => Boolean,
             // serializer = SparkEnv.get.serializer
             formats: Formats = RelayLike.this.formats
           ): MessageRelayParam[Obj] = new MessageRelayParam(this, parent, name, doc, isValid, formats)

  def Param(parent: String, name: String, doc: String): MessageRelayParam[Obj] =
    Param(parent, name, doc, (_: Obj) => true)

  def Param(parent: Identifiable, name: String, doc: String, isValid: Obj => Boolean): MessageRelayParam[Obj] =
    Param(parent.uid, name, doc, isValid)

  def Param(parent: Identifiable, name: String, doc: String): MessageRelayParam[Obj] =
    Param(parent.uid, name, doc)
}

abstract class MessageRelay[Obj] extends RelayLike[Obj] {

  override def mf: Manifest[M] = intrinsicManifestTry.get

  //TODO: it only works if impl of MessageRelay is an object
  // maybe switching to M.<get companion class>?
  final val intrinsicManifestTry: Try[Manifest[this.M]] = Try{

    val clazz = this.getClass
    val name = clazz.getName
    val modifiedName = name + "M"
    val reprClazz = Utils.classForName(modifiedName)

    Manifest.classType[this.M](reprClazz)
  }

  override def reader: MessageReader[M] = this.MReader
  object MReader extends MessageReader[M]()(mf) {
    override implicit def formats: Formats = MessageRelay.this.formats

    override def _read(jf: JField, formats: Formats, mf: Manifest[M]) =
      MessageRelay.this._read(jf, formats, mf)
  }
}
