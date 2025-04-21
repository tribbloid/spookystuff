package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.execution.FlatMapPlan
import com.tribbloids.spookystuff.linq.Rec
import com.tribbloids.spookystuff.linq.internal.ElementWisePoly
import com.tribbloids.spookystuff.row.AgentRow

import scala.reflect.ClassTag

/**
  * Created by peng on 2/12/15.
  */
trait DataViewRecInterface[D] { // TODO: should be mixed into DataView
  self: DataView[D] =>

  object withColumnsMany {

    def apply[
        CL, // type of left operand columns
        R,
        CR, // type of right operand columns
        O: ClassTag
    ](
        fn: FlatMapPlan.FlatMap._Fn[D, R],
        downSampling: DownSampling = ctx.conf.selectSampling
    )(
        implicit
        lemma1Left: Rec.ofData.Lemma[D, CL],
        lemma1Right: Rec.ofData.Lemma[R, CR],
        lemma2: ElementWisePoly.ifNoConflict.Lemma[(CL, CR), O]
    ): DataView[O] = {

      self.flatMap(
        { (v: AgentRow[D]) =>
          val left = lemma1Left(v.data)
          val rightValues = fn(v).toSeq
          val rights = rightValues.map(v => lemma1Right(v))

          val tuples = rights.map { right =>
            left -> right
          }

          tuples.map { v =>
            val result = lemma2(v)
            result
          }
        },
        downSampling
      )
    }
  }

  object withColumns {

    def apply[
        CL, // type of left operand columns
        R,
        CR, // type of right operand columns
        O: ClassTag
    ](
        fn: FlatMapPlan.Map._Fn[D, R],
        downSampling: DownSampling = ctx.conf.selectSampling
    )(
        implicit
        lemma1Left: Rec.ofData.Lemma[D, CL],
        lemma1Right: Rec.ofData.Lemma[R, CR],
        lemma2: ElementWisePoly.ifNoConflict.Lemma[(CL, CR), O]
    ): DataView[O] = {

      withColumnsMany.apply(row => Seq(fn(row)), downSampling)
    }
  }
}
