package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.util.Utils
import org.json4s.jackson.JsonMethods._
import org.json4s.{Extraction, Formats, JValue}

import scala.language.implicitConversions
import scala.xml.{NodeSeq, XML}

//mixin to allow converting to  a simple case class and back
//used to delegate ser/de tasks (from/to xml, json & dataset encoded type) to the case class with a fixed schema
//all subclasses must be objects otherwise Spark SQL can't find schema for Repr
abstract class MessageRelay[T] {

  //has to be a case class
  type M <: Message

  def toMessage(v: T): M

  final def toMLWriter(v: T) = toMessage(v).MLWriter
  final def toMLReader = MLReader

  trait ObjectMixin {
    self: T =>

    final def toMessage: M = MessageRelay.this.toMessage(self)
  }

  case object MLReader extends MLReader[T] {

    def outer = MessageRelay.this

    //TODO: need impl
    override def load(path: String): T = {
      //      val metadata = DefaultParamsReader.loadMetadata(path, sc)
      //      val cls = Utils.classForName(metadata.className)
      //      val instance =
      //        cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
      //      DefaultParamsReader.getAndSetParams(instance, metadata)
      //      instance.asInstanceOf[T]
      ???
    }
  }

  val xmlFormat: Formats = Xml.defaultFormats

  // the following Impl confines all subclasses to be objects
  //TODO: it only works if impl of MessageRelay is an object
  lazy val mf: Manifest[this.M] = {

    val clazz = this.getClass
    val name = clazz.getName
    val modifiedName = name + "M"
    val reprClazz = Utils.classForName(modifiedName)

    Manifest.classType[this.M](reprClazz)
  }

  def fromJValue(jv: JValue): M = {

    Extraction.extract[M](jv)(xmlFormat, mf)
  }
  def fromJSON(json: String): M = fromJValue(parse(json))

  def fromNodeSeq(ns: NodeSeq): M = {
    val jv = Xml.toJson(ns)

    fromJValue(jv.children.head)
  }
  def fromXML(xml: String): M = {
    val ns = XML.loadString(xml)

    fromNodeSeq(ns)
  }
}

trait Message extends Product with Serializable {

  import org.json4s.JsonDSL._

  //  implicit def nodeSeqToElem(xml: NodeSeq): Elem = {
  //    XML.loadString(xml.toString())
  //  }

  def formatted: Any = this

  def jValue(implicit formats: Formats = formats): JValue = Extraction.decompose(formatted)
  def compactJSON(implicit formats: Formats = formats): String = compact(render(jValue))
  def prettyJSON(implicit formats: Formats = formats): String = pretty(render(jValue))
  def toJSON(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyJSON(formats)
    else compactJSON(formats)
  }

  def formats: Formats = Xml.defaultFormats

  def toXMLNode(implicit formats: Formats = formats): NodeSeq = Xml.toXml(formatted.getClass.getSimpleName -> jValue)
  def compactXML(implicit formats: Formats = formats): String = toXMLNode.toString()
  def prettyXML(implicit formats: Formats = formats): String = Xml.defaultXMLPrinter.formatNodes(toXMLNode)
  def toXMLStr(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyXML
    else compactXML
  }

  case object MLWriter extends MLWriter with Serializable {

    def outer = Message.this

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

class MessageParams(
                     val uid: String
                   ) extends Params {

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)
}

case class MessageWrapper(
                           override val formatted: Any
                         ) extends Message

trait MessageRepr[T] extends Message {

  def toObject: T
}