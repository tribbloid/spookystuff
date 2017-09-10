package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.execution.ExecutionContext

/**
  * Created by peng on 15/07/17.
  */
object AutoSnapshotRewriter extends Rewriter[Trace] {

  override def rewrite(v: Trace, ec: ExecutionContext): Trace = {
    if (v.last.hasOutput) v
    else v :+ Snapshot() //Don't use singleton, otherwise will flush timestamp and name
  }
}
