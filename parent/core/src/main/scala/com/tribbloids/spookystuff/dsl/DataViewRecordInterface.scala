package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.execution.FlatMapPlan
import com.tribbloids.spookystuff.linq.Record
import com.tribbloids.spookystuff.linq.internal.ElementWisePoly
import com.tribbloids.spookystuff.row.AgentRow

/**
  * Created by peng on 2/12/15.
  */
trait DataViewRecordInterface[D] {
  self: DataView[D] =>

  object withColumns {

    def apply[V, L, R](
        fn: FlatMapPlan.Map._Fn[D, V],
        downSampling: DownSampling = ctx.conf.selectSampling
    )(
        implicit
        lemma1Left: Record.ofData.Lemma[D, L],
        lemma1Right: Record.ofData.Lemma[V, R],
        lemma2: ElementWisePoly.ifNoConflict.Lemma.At[(L, R)]
    ): DataView[lemma2.Out] = {

      self.map(
        { (v: AgentRow[D]) =>
          val left = lemma1Left(v.data)
          val rightValue: V = fn(v)
          val right = lemma1Right(rightValue)

          lemma2(left -> right)
        },
        downSampling
      )
    }
  }
}
