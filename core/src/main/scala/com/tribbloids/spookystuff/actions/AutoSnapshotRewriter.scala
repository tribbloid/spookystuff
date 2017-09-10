package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.row.DataRowSchema

/**
  * Created by peng on 15/07/17.
  */
object AutoSnapshotRewriter extends TraceRewriter {

  override def rewriteGlobally(v: Trace, schema: DataRowSchema): Trace = {
    val n = v.count(_.isInstanceOf[WebInteraction])
    if (n > 0 && v.last.hasOutput) v
    else v :+ Snapshot() //Don't use singleton, otherwise will flush timestamp and name
  }
}
