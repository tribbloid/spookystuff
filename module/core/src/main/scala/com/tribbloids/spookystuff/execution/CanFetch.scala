package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.HasTraceSet
import com.tribbloids.spookystuff.execution.FetchPlan.Batch

import scala.reflect.ClassTag

trait CanFetch[
    ON, // accepting
    I, // input row
    O // output row
] extends Serializable {

  def normaliseOutput(inputRow: I, on: ON): Batch[O]

  def cTag: ClassTag[O]
}

object CanFetch {

  import FetchPlan.*

  implicit def _onTraceSet[T <: HasTraceSet, I](
      implicit
      _cTag: ClassTag[I]
  ): CanFetch[T, I, I] = {

    new CanFetch[T, I, I] {

      override def normaliseOutput(inputRow: I, on: T): Batch[I] = {

        on.traceSet.toSeq.map { trace =>
          (trace, inputRow)
        }
      }

      override def cTag: ClassTag[I] = _cTag
    }
  }

  implicit def _onCollection[S[T] <: IterableOnce[T], T <: HasTraceSet, I](
      implicit
      _cTag: ClassTag[I]
  ): CanFetch[S[T], I, I] = {

    new CanFetch[S[T], I, I] {

      override def normaliseOutput(inputRow: I, on: S[T]): Batch[I] = {

        on.iterator.toSeq
          .flatMap { v =>
            v.traceSet
          }
          .distinct
          .map { trace =>
            (trace, inputRow)
          }
      }

      override def cTag: ClassTag[I] = _cTag
    }
  }

  implicit def _onCollectionWithData[T <: HasTraceSet, I, O](
      implicit
      _cTag: ClassTag[O]
  ): CanFetch[Seq[(T, O)], I, O] = {

    new CanFetch[Seq[(T, O)], I, O] {

      override def normaliseOutput(inputRow: I, on: Seq[(T, O)]): Batch[O] = {

        on.flatMap {

          case (traceSet, outputRow) =>

            _onTraceSet[T, O].normaliseOutput(outputRow, traceSet)
        }
      }

      override def cTag: ClassTag[O] = _cTag
    }
  }
}
