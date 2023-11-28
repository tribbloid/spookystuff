package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.utils.io.ResourceMetadata

//Merely a placeholder if a conditional block is not applicable
case class NoDoc(
    backtrace: Trace,
    override val timeMillis: Long = System.currentTimeMillis(),
    override val cacheLevel: DocCacheLevel.Value = DocCacheLevel.All,
    override val metadata: ResourceMetadata = ResourceMetadata.empty
) extends Serializable
    with Fetched.Success {

  @transient override lazy val uid: DocUID = DocUID(backtrace, null)()

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): NoDoc.this.type = this.copy(backtrace = uid.backtrace, cacheLevel = cacheLevel).asInstanceOf[this.type]

  override type RootType = Unit
  override def root: Unit = {}
}
