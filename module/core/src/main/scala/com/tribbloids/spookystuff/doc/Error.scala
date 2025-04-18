package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.SpookyException
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.io.ResourceMetadata

object Error {

  // TODO: has a lot of duplications, need to be abstracted away

  trait ErrorWithDoc extends SpookyException.HasDoc with Observation.Error {

    override def timeMillis: Long = doc.timeMillis

    override def uid: DocUID = doc.uid

    override def cacheLevel: DocCacheLevel.Value = doc.cacheLevel

    override type RootType = doc.RootType
    override def root: Node = doc.root

    override def metadata: ResourceMetadata = doc.metadata

    override def docForAuditing: Option[Doc] = Some(doc)
  }

//  case class FetchError(
//      doc: Doc,
//      header: String = "",
//      override val getCause: Exception = null
//  ) extends ErrorWithDoc {
//
//    override def getMessage_simple: String = {
//
//      header + doc.root.formattedCode
//        .map(
//          "\n" + _
//        )
//        .getOrElse("")
//    }
//
//    override def updated(
//        uid: DocUID = this.uid,
//        cacheLevel: DocCacheLevel.Value = this.cacheLevel
//    ): FetchError = {
//      this.copy(doc = doc.updated(uid, cacheLevel))
//    }
//  }

  case class ConversionError(
      doc: Doc,
      cause: Throwable = null
  ) extends ErrorWithDoc {

    override def getMessage_simple: String = {

      doc.root.formattedCode
        .map(
          "\n" + _
        )
        .getOrElse("")
    }

    override def updated(
        uid: DocUID = this.uid,
        cacheLevel: DocCacheLevel.Value = this.cacheLevel
    ): ConversionError = {
      this.copy(doc = doc.updated(uid, cacheLevel))
    }
  }

  case class ValidationError(
      summary: String = "",
      doc: Doc,
      cause: Throwable = null
  ) extends ErrorWithDoc {
    // thrown when fetched doc is ill-formed (e.g. has Http Code other than 2XX)

    override def getMessage_simple: String = {

      summary
    }

    override def updated(
        uid: DocUID = this.uid,
        cacheLevel: DocCacheLevel.Value = this.cacheLevel
    ): ValidationError = {
      this.copy(doc = doc.updated(uid, cacheLevel))
    }
  }
}
