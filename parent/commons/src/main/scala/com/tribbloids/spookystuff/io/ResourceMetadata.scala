package com.tribbloids.spookystuff.io

import com.tribbloids.spookystuff.commons.data.EAVSchema

object ResourceMetadata extends EAVSchema {

  implicit final class EAV(
      override val internal: collection.Map[String, Any]
  ) extends EAVMixin.CaseInsensitive {

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

    def normalise(): EAV = {

      val newInternal = internal.view.mapValues {
        case v: String => v
        case v: Number => v
        case v         => "" + v
      }.toMap
      EAV(newInternal)
    }

  }
}
