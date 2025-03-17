package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.io.ResourceMetadata

case class ConversionError(
    delegate: Doc,
    cause: Throwable = null
) extends Observation.Failure {

  override def timeMillis: Long = delegate.timeMillis

  override def uid: DocUID = delegate.uid

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): ConversionError = {
    this.copy(delegate = delegate.updated(uid, cacheLevel))
  }

  override def cacheLevel: DocCacheLevel.Value = delegate.cacheLevel

  override type RootType = delegate.RootType
  override def root: Unstructured = delegate.root

  override def metadata: ResourceMetadata = delegate.metadata

  override def docForAuditing: Option[Doc] = Some(delegate)
}
