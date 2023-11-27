package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.{RewriteRule, Trace}

/**
  * Created by peng on 15/07/17.
  */
object AutoSnapshotRule extends RewriteRule[Trace] {

  override def rewrite(v: Trace): Seq[Trace] = {
    val n = v.count(_.isInstanceOf[WebInteraction])
    if (n > 0 && v.last.hasOutput) Seq(v)
    else Seq(v +> Snapshot()) // Don't use singleton, otherwise will flush timestamp and name
  }
}
