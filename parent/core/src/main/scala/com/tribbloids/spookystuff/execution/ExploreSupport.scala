package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.commons.Types.Reduce
import com.tribbloids.spookystuff.row.Data.WithLineage

trait ExploreSupport[D] {

  protected type Lineage = WithLineage[D]

  protected type Batch = Vector[WithLineage[D]]

  trait Reducer extends Reduce[Batch] with Serializable {

    def reduce(
        v1: Batch,
        v2: Batch
    ): Batch

    final override def apply(
        old: Batch,
        neo: Batch
    ): Batch = reduce(old, neo)
  }

}
