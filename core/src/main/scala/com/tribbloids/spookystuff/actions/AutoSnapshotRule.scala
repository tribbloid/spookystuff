package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.row.SpookySchema

/**
  * Created by peng on 15/07/17.
  */
object AutoSnapshotRule extends RewriteRule[Trace] {

  override def rewrite(v: Trace, schema: SpookySchema): Trace = {
    val n = v.count(_.isInstanceOf[WebInteraction])
    if (n > 0 && v.last.hasOutput) v
    else v :+ Snapshot() //Don't use singleton, otherwise will flush timestamp and name
  }
}
