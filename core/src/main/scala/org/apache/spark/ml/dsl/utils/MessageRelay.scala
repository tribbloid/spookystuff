package org.apache.spark.ml.dsl.utils

import org.apache.spark.annotation.DeveloperApi
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

  implicit val formats: Formats = Xml.defaultFormats

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

  final def toMLWriter(v: Obj) = toMessage(v).MLWriter

  trait HasRelay {
    self: Obj =>

    final def toMessage: Message = MessageRelay.this.toMessage(self)
    final def toMessageValue: MessageRelay.this.M = toMessage.value.asInstanceOf[MessageRelay.this.M]
  }

  class UDT extends UserDefinedType[Obj] {

    override def sqlType: DataType = ???

    override def serialize(obj: Any): Any = ???

    override def deserialize(datum: Any): Obj = ???

    override def userClass: Class[Obj] = ???
  }

  final def toMLReader: MLReader[Obj] = MLReader
  case object MLReader extends MLReader[Obj] {

    def outer = MessageRelay.this

    //TODO: need impl
    override def load(path: String): Obj = {
      //      val metadata = DefaultParamsReader.loadMetadata(path, sc)
      //      val cls = Utils.classForName(metadata.className)
      //      val instance =
      //        cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
      //      DefaultParamsReader.getAndSetParams(instance, metadata)
      //      instance.asInstanceOf[T]
      ???
    }
  }

  def Param(
             parent: String,
             name: String,
             doc: String,
             isValid: Obj => Boolean,
             // serializer = SparkEnv.get.serializer
             formats: Formats = Xml.defaultFormats
           ): Param = new Param(parent, name, doc, isValid, formats)

  def Param(parent: String, name: String, doc: String): Param =
    Param(parent, name, doc, (_: Obj) => true)

  def Param(parent: Identifiable, name: String, doc: String, isValid: Obj => Boolean): Param =
    Param(parent.uid, name, doc, isValid)

  def Param(parent: Identifiable, name: String, doc: String): Param =
    Param(parent.uid, name, doc)
  /**
    * :: DeveloperApi ::
    * ML Param only supports string & vectors, this class extends support to all objects
    */
  @DeveloperApi
  class Param(
               parent: String,
               name: String,
               doc: String,
               isValid: Obj => Boolean,
               // serializer = SparkEnv.get.serializer
               formats: Formats
             ) extends org.apache.spark.ml.param.Param[Obj](parent, name, doc, isValid) {


    /** Creates a param pair with the given value (for Java). */
    //    override def w(value: M): ParamPair[M] = super.w(value)

    override def jsonEncode(value: Obj): String = {

      toMessage(value)
        .compactJSON(MessageRelay.this.formats)
    }

    override def jsonDecode(json: String): Obj = {

      val message: M = MessageRelay.this.fromJSON(json)
      message match {
        case v: MessageRepr[_] =>
          v.toObject.asInstanceOf[Obj]
        case _ =>
          throw new UnsupportedOperationException("jsonDecode is not implemented")
      }
    }
  }
}

class MessageReader[Obj](
                          implicit override val mf: Manifest[Obj]
                        ) extends MessageRelay[Obj] {
  type M = Obj

  override implicit val formats: Formats = Xml.defaultFormats +
    DurationJSONSerializer +
    FallbackJSONSerializer

  override def toMessage(v: Obj) = new MessageRepr[Obj] {
//    override type M = Obj
    override def toObject: Obj = v
  }
}

trait Message extends Serializable {

  def value: Any = this
  def formats: Formats = Xml.defaultFormats

  import org.json4s.JsonDSL._

  def jValue(implicit formats: Formats = formats): JValue = Extraction.decompose(value)
  def compactJSON(implicit formats: Formats = formats): String = compact(render(jValue))
  def prettyJSON(implicit formats: Formats = formats): String = pretty(render(jValue))
  def toJSON(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyJSON(formats)
    else compactJSON(formats)
  }

  def toXMLNode(implicit formats: Formats = formats): NodeSeq = Xml.toXml(value.getClass.getSimpleName -> jValue)
  def compactXML(implicit formats: Formats = formats): String = toXMLNode.toString()
  def prettyXML(implicit formats: Formats = formats): String = Xml.defaultXMLPrinter.formatNodes(toXMLNode)
  def toXMLStr(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyXML
    else compactXML
  }

  case object MLWriter extends MLWriter with Serializable {

    def message = Message.this

    //    def saveJSON(path: String): Unit = {
    //      val resolver = HDFSResolver(sc.hadoopConfiguration)
    //
    //      resolver.output(path, overwrite = true){
    //        os =>
    //          os.write(StructRepr.this.prettyJSON.getBytes("UTF-8"))
    //      }
    //    }

    override protected def saveImpl(path: String): Unit = {

      val instance = new MessageParams(Identifiable.randomUID(Message.this.getClass.getSimpleName))

      DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata = Some("metadata" -> Message.this.jValue))

      // Save stages
      //    val stagesDir = new Path(path, "stages").toString
      //    stages.zipWithIndex.foreach { case (stage: MLWritable, idx: Int) =>
      //      stage.write.save(getStagePath(stage.uid, idx, stages.length, stagesDir))
      //    }
    }
  }
}
trait MessageRepr[T] extends Message {

  def toObject: T
}
case class MessageView[MM](
                            override val value: MM,
                            override val formats: Formats = Xml.defaultFormats
                          ) extends Message {

}

class MessageParams(
                     val uid: String
                   ) extends Params {

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)
}
