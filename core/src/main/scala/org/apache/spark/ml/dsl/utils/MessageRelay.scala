package org.apache.spark.ml.dsl.utils

import org.apache.http.entity.StringEntity
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{DataType, UserDefinedType}
import org.apache.spark.util.Utils
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.implicitConversions
import scala.util.Try
import scala.xml.{NodeSeq, XML}

//mixin to allow converting to  a simple case class and back
//used to delegate ser/de tasks (from/to xml, json & dataset encoded type) to the case class with a fixed schema
//all subclasses must be objects otherwise Spark SQL can't find schema for Repr
abstract class MessageRelay[Obj] {

  implicit def formats: Formats = Xml.defaultFormats

  type M

  implicit def mf: Manifest[this.M] = intrinsicManifestTry.get

  //TODO: it only works if impl of MessageRelay is an object
  final val intrinsicManifestTry: Try[Manifest[this.M]] = Try{

    val clazz = this.getClass
    val name = clazz.getName
    val modifiedName = name + "M"
    val reprClazz = Utils.classForName(modifiedName)

    Manifest.classType[this.M](reprClazz)
  }

  def _fromJValue[T: Manifest](jv: JValue): T = {

    Extraction.extract[T](jv)
  }
  def _fromJSON[T: Manifest](json: String): T = _fromJValue[T](parse(json))

  def _fromXMLNode[T: Manifest](ns: NodeSeq): T = {
    val jv = Xml.toJson(ns)

    _fromJValue[T](jv.children.head)
  }
  def _fromXML[T: Manifest](xml: String): T = {
    val bomRemoved = xml.replaceAll("[^\\x20-\\x7e]", "").trim //remove BOM (byte order mark)
    val prologRemoved = bomRemoved.replaceFirst("[^<]*(?=<)","")
    val ns = XML.loadString(prologRemoved)

    _fromXMLNode[T](ns)
  }

  def fromJValue(jv: JValue): M = _fromJValue[M](jv)
  def fromJSON(json: String): M = _fromJSON[M](json)

  def fromXMLNode(ns: NodeSeq): M = _fromXMLNode[M](ns)
  def fromXML(xml: String): M = _fromXML[M](xml)

  def toMessage(v: Obj): Message
  final def toMessageValue(v: Obj): MessageRelay.this.M = toMessage(v).value.asInstanceOf[MessageRelay.this.M]

  trait HasRelay extends HasMessage {
    self: Obj =>

    override def formats = MessageRelay.this.formats

    final def toMessage: Message = MessageRelay.this.toMessage(self)
  }

  class UDT extends UserDefinedType[Obj] {

    override def sqlType: DataType = ???

    override def serialize(obj: Any): Any = ???

    override def deserialize(datum: Any): Obj = ???

    override def userClass: Class[Obj] = ???
  }

  def Param(
             parent: String,
             name: String,
             doc: String,
             isValid: Obj => Boolean,
             // serializer = SparkEnv.get.serializer
             formats: Formats = MessageRelay.this.formats
           ): MessageRelayParam[Obj] = new MessageRelayParam(this, parent, name, doc, isValid, formats)

  def Param(parent: String, name: String, doc: String): MessageRelayParam[Obj] =
    Param(parent, name, doc, (_: Obj) => true)

  def Param(parent: Identifiable, name: String, doc: String, isValid: Obj => Boolean): MessageRelayParam[Obj] =
    Param(parent.uid, name, doc, isValid)

  def Param(parent: Identifiable, name: String, doc: String): MessageRelayParam[Obj] =
    Param(parent.uid, name, doc)
}

/**
  * a simple MessageRelay that use object directly as Message
  */
class MessageReader[Obj](
                          implicit override val mf: Manifest[Obj]
                        ) extends MessageRelay[Obj] {
  type M = Obj

  override def toMessage(v: Obj) = new MessageView[Obj](v, MessageReader.this.formats)
}

object GenericMessageReader extends MessageReader[Any]

trait HasMessage extends Serializable {

  def formats: Formats = Xml.defaultFormats

  def toMessage: Message
  def toMessageValue: Any = toMessage.value
}

trait Message extends HasMessage {

  def toMessage = this

  def value: Any = this

  import org.json4s.JsonDSL._

  def toJValue(implicit formats: Formats = formats): JValue = Extraction.decompose(value)
  def compactJSON(implicit formats: Formats = formats): String = compact(render(toJValue))
  def prettyJSON(implicit formats: Formats = formats): String = pretty(render(toJValue))
  def toJSON(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyJSON(formats)
    else compactJSON(formats)
  }
  def toHTTPEntity(implicit formats: Formats = formats): StringEntity = {
    val requestEntity = new StringEntity(
      this.toJSON()
    )
    requestEntity
  }

  def toXMLNode(implicit formats: Formats = formats): NodeSeq = Xml.toXml(value.getClass.getSimpleName -> toJValue)
  def compactXML(implicit formats: Formats = formats): String = toXMLNode.toString()
  def prettyXML(implicit formats: Formats = formats): String = Xml.defaultXMLPrinter.formatNodes(toXMLNode)
  def toXMLStr(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyXML
    else compactXML
  }

  def cast[T: Manifest](implicit formats: Formats = formats) = {
    GenericMessageReader._fromJValue[T](toJValue(formats))
  }
}

trait MessageRepr[Obj] extends Message {

  def toObject: Obj
}

//TODO: remove type param, useless!
case class MessageView[M](
                           override val value: M,
                           override val formats: Formats = Xml.defaultFormats
                         ) extends Message {
}

class MessageParams(
                     val uid: String
                   ) extends Params {

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)
}
