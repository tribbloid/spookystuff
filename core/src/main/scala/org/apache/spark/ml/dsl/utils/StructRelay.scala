package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.utils.HDFSResolver
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.util.Utils
import org.json4s.jackson.JsonMethods._
import org.json4s.{Extraction, Formats, JObject, JValue, Serializer}

import scala.language.implicitConversions
import scala.xml.{NodeSeq, XML}

//mixin to allow converting to  a simple case class and back
//used to delegate ser/de tasks (from/to xml, json & dataset encoded type) to the case class with a fixed schema
//all subclasses must be objects otherwise Spark SQL can't find schema for Repr
abstract class StructRelay[T] {

  //has to be a case class
  type Repr <: StructRepr[T]

  def toRepr(v: T): Repr

  final def toMLWriter(v: T) = toRepr(v).MLWriter
  final def toMLReader = MLReader

  trait ToReprMixin {
    self: T =>

    final def toRepr: Repr = StructRelay.this.toRepr(self)
  }

  case object MLReader extends MLReader[T] {

    def outer = StructRelay.this

    @org.apache.spark.annotation.Since("1.6.0")
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

  def extraSer: Seq[Serializer[_]] = Nil
  val format: Formats = Xml.defaultFormats ++ extraSer

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
    val ns = XML.loadString(xml)

    fromNodeSeq(ns)
  }
}

trait StructRepr[T] extends Product with Serializable {

  def toSelf: T

  def xmlRoot: String = "root"

  def extraSer: Seq[Serializer[_]] = Nil
  final implicit val format: Formats = Xml.defaultFormats ++ extraSer

  def jValue: JObject = Extraction.decompose(this).asInstanceOf[JObject]
  def compactJSON = compact(render(jValue))
  def prettyJSON = pretty(render(jValue))

  import org.json4s.JsonDSL._

  def xmlNode = Xml.toXml(xmlRoot -> jValue)
  def compactXML = xmlNode.toString()
  def prettyXML = Xml.defaultXMLPrinter.formatNodes(xmlNode)

  case object MLWriter extends MLWriter {

    def outer = StructRepr.this

    def saveJSON(path: String): Unit = {
      val resolver = HDFSResolver(sc.hadoopConfiguration)

      resolver.output(path, overwrite = true){
        os =>
          os.write(StructRepr.this.prettyJSON.getBytes("UTF-8"))
      }
    }

    @org.apache.spark.annotation.Since("1.6.0") override protected
    def saveImpl(path: String): Unit = {

      val instance = new StructParams()

      DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata = Some(StructRepr.this.jValue))

      // Save stages
      //    val stagesDir = new Path(path, "stages").toString
      //    stages.zipWithIndex.foreach { case (stage: MLWritable, idx: Int) =>
      //      stage.write.save(getStagePath(stage.uid, idx, stages.length, stagesDir))
      //    }
    }


  }

  class StructParams(
                      val uid: String = Identifiable.randomUID(StructRepr.this.getClass.getSimpleName)
                    ) extends Params {

    override def copy(extra: ParamMap): Params = this.defaultCopy(extra)
  }
}