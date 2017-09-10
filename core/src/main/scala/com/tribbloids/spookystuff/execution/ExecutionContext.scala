package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext

/**
  * Created by peng on 10/07/17.
  */
//TODO: merge into DataRowSchema?
case class ExecutionContext(
                             spooky: SpookyContext,
                             @transient scratchRDDs: ScratchRDDs = new ScratchRDDs()
                           ) {

  def ++(b: ExecutionContext) = {
    //    assert(this.spooky == b.spooky,
    //      "cannot merge execution plans due to diverging SpookyContext")

    import scratchRDDs._
    val bb = b.scratchRDDs
    this.copy(
      scratchRDDs = new ScratchRDDs(
        tempTables = <+>(bb, _.tempTables),
        tempRDDs = <+>(bb, _.tempRDDs),
        tempDFs = <+>(bb, _.tempDFs)
      )
    )
  }
}