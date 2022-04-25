package com.tribbloids.spookystuff.actions

/**
  * Created by peng on 15/07/17.
  */
object AutoSnapshotRule extends RewriteRule[TraceView] {

  override def rewrite(v: TraceView): Seq[TraceView] = {
    val n = v.count(_.isInstanceOf[WebInteraction])
    if (n > 0 && v.last.hasOutput) Seq(v)
    else Seq(v +> Snapshot()) //Don't use singleton, otherwise will flush timestamp and name
  }
}
