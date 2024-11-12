package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.io.ResourceMetadata

//produced in dryrun, only for shuffling to distribute locality group
case class DryRun(
    backtrace: Trace,
    override val timeMillis: Long = System.currentTimeMillis()
) extends Serializable
    with Observation.Success {

  override def cacheLevel: DocCacheLevel.Value = DocCacheLevel.NoCache
  override def metadata: ResourceMetadata = ResourceMetadata.empty

  @transient override lazy val uid: DocUID = DocUID(backtrace, null)()

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): DryRun.this.type = this.copy(backtrace = uid.backtrace).asInstanceOf[this.type]

  override type RootType = Unit
  override def root: Unit = {}

  override def docForAuditing: Option[Doc] = None
}
