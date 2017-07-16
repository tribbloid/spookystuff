package com.tribbloids.spookystuff.actions

/**
  * Created by peng on 15/07/17.
  */
object AutoSnapshotRewriter extends Rewriter[Trace] {

  override def apply(v1: Trace): Trace = {
    if (v1.last.hasOutput) v1
    else v1 :+ Snapshot() //Don't use singleton, otherwise will flush timestamp and name
  }
}
