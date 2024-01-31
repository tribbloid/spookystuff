package com.tribbloids.spookystuff.relay.io

import com.tribbloids.spookystuff.relay.xml.{XMLFormats, Xml}
import com.tribbloids.spookystuff.relay.{IR, TreeIR}
import org.json4s.{Extraction, Formats, JField, JsonAST}

trait Decoder[+_IR <: IR] extends (JField => _IR) {

  def defaultRootTag: String
}

object Decoder {

  case class Plain[D](
      formats: Formats = XMLFormats.defaultFormats
  )(
      implicit
      typeInfo: Manifest[D]
  ) extends Decoder[TreeIR.Leaf[D]] {

    def apply(jf: JField): TreeIR.Leaf[D] = {

      val msg = Extraction.extract[D](jf._2)(formats, typeInfo)
      val ir = TreeIR.Builder(Some(jf._1)).leaf(msg)
      ir
    }

    override def defaultRootTag: String = typeInfo.runtimeClass.getSimpleName
  }

  // TODO: this implementation is incomplete
  case class AnyTree(
      formats: Formats = XMLFormats.defaultFormats
  ) extends Decoder[TreeIR[Any]] {

    override def apply(jf: (String, JsonAST.JValue)): TreeIR[Any] = {

      val msg = Extraction.extract[Any](jf._2)(formats, Manifest.Any)

      val raw = TreeIR.leaf(msg)

      raw.explode.explodeStringMap()
    }

    override def defaultRootTag: String = Xml.ROOT
  }
}
