package com.tribbloids.spookystuff.doc

import org.apache.hadoop.shaded.org.apache.http.entity.ContentType

import java.nio.charset.Charset
import scala.language.implicitConversions

case class ContentTypeView(
    @transient _contentType: ContentType
) {

  private val noCharset = _contentType.withCharset(null: Charset)

  private val charsetName: Option[String] = _contentType.getCharset match {
    case null => None
    case v    => Some(v.name())
  }

  @transient lazy val contentType: ContentType = {
    charsetName match {
      case None    => noCharset
      case Some(v) => noCharset.withCharset(v)
    }
  }
}

object ContentTypeView {

//  implicit def box(v: ContentType): SerializableContentTypeWrapper = SerializableContentTypeWrapper(v)

  implicit def unbox(v: ContentTypeView): ContentType = v.contentType
}
