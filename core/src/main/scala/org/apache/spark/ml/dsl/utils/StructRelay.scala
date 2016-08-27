package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.util.Utils
import org.json4s.jackson.JsonMethods._
import org.json4s.{Extraction, Formats, JObject, JValue, Serializer}

import scala.language.implicitConversions
import scala.xml.{Elem, NodeSeq, XML}

//mixin to allow converting to  a simple case class and back
//used to delegate ser/de tasks (from/to xml, json & dataset encoded type) to the case class with a fixed schema
//all subclasses must be objects otherwise Spark SQL can't find schema for Repr
abstract class StructRelay[T] {

  final def XML_ROOT: String = "root"

  //has to be a case class
  type Repr <: StructRepr[T]

  def toRepr(v: T): Repr

  final def toMLWriter(v: T) = toRepr(v).MLWriter
  final def toMLReader = MLReader

  trait SelfMixin {
    self: T =>

    final def toRepr: Repr = StructRelay.this.toRepr(self)
  }

  case object MLReader extends MLReader[T] {

    def outer = StructRelay.this

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

  def extraJSONSerializers: Seq[Serializer[_]] = Nil
  val format: Formats = Xml.defaultFormats ++ extraJSONSerializers

  // the following Impl confines all subclasses to be objects
  lazy val mf: Manifest[this.Repr] = {

    val clazz = this.getClass
    val name = clazz.getName
    val modifiedName = name + "Repr"
    val reprClazz = Utils.classForName(modifiedName)

    Manifest.classType[this.Repr](reprClazz)
  }

  def fromJValue(jv: JValue): Repr = {

    Extraction.extract[Repr](jv)(format, mf)
  }
  def fromJSON(json: String): Repr = fromJValue(parse(json))

  def fromNodeSeq(ns: NodeSeq): Repr = {
    val jv = Xml.toJson(ns)

    fromJValue(jv.children.head)
  }
  def fromXML(xml: String): Repr = {
    val ns =
//      try {
      XML.loadString(xml)
//    }
//    catch {
//      case e: SAXParseException =>
//        XML.loadString("<root>" + xml + "</root>")
//    }

    fromNodeSeq(ns)
  }
}

trait Struct extends Product with Serializable {

  import org.json4s.JsonDSL._

  implicit def formats: Formats = Xml.defaultFormats

  implicit def nodeSeqToElem(xml: NodeSeq): Elem = {
    XML.loadString(xml.toString())
  }

  def jValue: JObject = Extraction.decompose(this).asInstanceOf[JObject]
  def compactJSON = compact(render(jValue))
  def prettyJSON = pretty(render(jValue))
  def toJSON(pretty: Boolean = true): String = {
    if (true) prettyJSON
    else compactJSON
  }

  def toXMLNode = Xml.toXml(this.getClass.getSimpleName -> jValue)
  def compactXML = toXMLNode.toString()
  def prettyXML = Xml.defaultXMLPrinter.formatNodes(toXMLNode)
  def toXMLStr(pretty: Boolean = true): String = {
    if (true) prettyXML
    else compactXML
  }

  case object MLWriter extends MLWriter {

    def outer = Struct.this

//    def saveJSON(path: String): Unit = {
//      val resolver = HDFSResolver(sc.hadoopConfiguration)
//
//      resolver.output(path, overwrite = true){
//        os =>
//          os.write(StructRepr.this.prettyJSON.getBytes("UTF-8"))
//      }
//    }

    override protected def saveImpl(path: String): Unit = {

      val instance = new StructParams()

      DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata = Some(Struct.this.jValue))

      // Save stages
      //    val stagesDir = new Path(path, "stages").toString
      //    stages.zipWithIndex.foreach { case (stage: MLWritable, idx: Int) =>
      //      stage.write.save(getStagePath(stage.uid, idx, stages.length, stagesDir))
      //    }
    }
  }

  class StructParams(
                      val uid: String = Identifiable.randomUID(Struct.this.getClass.getSimpleName)
                    ) extends Params {

    override def copy(extra: ParamMap): Params = this.defaultCopy(extra)
  }
}

trait StructRepr[T] extends Struct {

  def toSelf: T
}