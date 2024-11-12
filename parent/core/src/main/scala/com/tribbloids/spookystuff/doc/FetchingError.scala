package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.*
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.io.ResourceMetadata

case class FetchingError(
    delegate: Doc,
    header: String = "",
    override val cause: Throwable = null
) extends ActionException(
      header + delegate.root.formattedCode
        .map(
          "\n" + _
        )
        .getOrElse(""),
      cause
    )
    with Observation.Failure {

  override def timeMillis: Long = delegate.timeMillis

  override def uid: DocUID = delegate.uid

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): FetchingError.this.type = {
    this.copy(delegate = delegate.updated(uid, cacheLevel)).asInstanceOf[this.type]
  }

  override def cacheLevel: DocCacheLevel.Value = delegate.cacheLevel

  override type RootType = delegate.RootType
  override def root: Unstructured = delegate.root

  override def metadata: ResourceMetadata = delegate.metadata

  override def docForAuditing: Option[Doc] = Some(delegate)
}
