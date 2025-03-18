package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.io.ResourceMetadata

//Merely a placeholder if a conditional block is not applicable
case class NoDoc(
    backtrace: Trace,
    override val timeMillis: Long = System.currentTimeMillis(),
    override val cacheLevel: DocCacheLevel.Value = DocCacheLevel.All,
    override val metadata: ResourceMetadata = ResourceMetadata.empty
) extends Serializable
    with Observation.Success {

  @transient override lazy val uid: DocUID = DocUID(backtrace)()

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): NoDoc = this.copy(backtrace = uid.backtrace, cacheLevel = cacheLevel)

  override type RootType = Unit
  override def root: Unit = {}

  override def docForAuditing: Option[Doc] = None
}
