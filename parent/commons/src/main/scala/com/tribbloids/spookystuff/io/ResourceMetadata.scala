package com.tribbloids.spookystuff.io

import com.tribbloids.spookystuff.commons.data.EAVSystem

object ResourceMetadata extends EAVSystem {

  case class ^(
      override val internal: collection.Map[String, Any]
  ) extends EAV.CaseInsensitive {

    case object URI extends Attr[String]()

    case object Name extends Attr[String]()

    case object Type extends Attr[String]()

    case object ContentType extends Attr[String](List("content-type"))

    case object Length extends Attr[Long](List("Len"))

    case object StatusCode extends Attr[Int](List("status-code"))

    case object `isDir` extends Attr[Boolean]()

    @transient lazy val sortEvidence: Option[String] = {

      val result = URI.get
      result
    }
  }
}
