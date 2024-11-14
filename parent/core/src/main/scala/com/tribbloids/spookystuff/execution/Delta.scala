package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.row.*

trait Delta[I, O] extends Serializable {

  // TODO: too complex? Should follow DeltaPlan.Fn
  def repr(schema: SpookySchema): SquashedRow[I] :=> SquashedRow[O]

}

object Delta {

//  case class DeltaView[I, O](self: Delta[I, O])(
//      implicit
//      ordering: Ordering[O]
//  ) {
//
//    def apply
//  }

//  case class Repr[I, O](
//      fn: SquashedRow[I] :=> SquashedRow[O],
//      schema: SpookySchema
//  )

//  case class Composed[I, X, Y](
//      left: Delta[I, X],
//      right: Delta[X, Y]
//  ) extends Delta[I, Y] {
//
//    override def repr(schema: SpookySchema): SquashedRow[I] :=> SquashedRow[Y] = {
//
//      val leftRepr = left.repr(schema)
//      val rightRepr = right.repr(schema)
//
//      leftRepr.andThen(rightRepr)
//    }
//  }

//  case class FlatMap[I, O](
//      fn: FetchedRow[I] :=> Seq[O]
//  ) extends Delta[I, O] {
//
//    override def repr(schema: SpookySchema): SquashedRow[I] :=> SquashedRow[O] = :=> { squashedRow =>
//      val rows = squashedRow.withCtx(schema.ctx).unSquash
//
//      val nextData = rows.flatMap { row =>
//        fn(row).map { nextData =>
//          row.copy(data = nextData).dataWithScope
//        }
//      }
//      squashedRow.copy(dataSeq = nextData)
//    }
//  }

//
//  /**
//    * extract parts of each Page and insert into their respective context if a key already exist in old context it will
//    * be replaced with the new one.
//    *
//    * @return
//    *   new PageRowRDD
//    */
//  class Extract[
//      I,
//      IT <: Tuple,
//      N,
//      NT <: Tuple,
//      OT <: Tuple
//  ](
//      fn: FetchedRow[I] :=> Seq[N]
//  )(
//      implicit
//      ev1: TypedRow.ofData.=>>[I, IT],
//      ev2: TypedRow.ofData.=>>[N, NT]
//  ) extends Delta[OT] {
//
//    val resolver: childSchema.Resolver = childSchema.newResolver
//    val _exs: Seq[Resolved[Any]] = resolver.include(exs: _*)
//
//    override lazy val repr: SquashedRow :=> SquashedRow = Impl { v =>
//      {
//        v.withCtx(outputSchema.spooky): v._WithCtx
//      }.extract(_exs: _*)
//    }
//  }

//  case class ExplodeData[I](
//      onField: Field,
//      ordinalField: Field,
//      sampler: Sampler[Any],
//      forkType: ForkType
//  ) extends Delta[I] {
//
//    val resolver: childSchema.Resolver = childSchema.newResolver
//
//    val _on: TypedField = {
//      val flattenType = CatalystTypeOps(Get(onField).resolveType(childSchema)).unboxArrayOrMap
//      val tf = TypedField(onField.!!, flattenType)
//
//      resolver.includeTyped(tf).head
//    }
//
//    val effectiveOrdinalField: Field = Option(ordinalField) match {
//      case Some(ff) =>
//        ff.copy(isOrdinal = true)
//      case None =>
//        Field(_on.self.name + "_ordinal", isTransient = true, isOrdinal = true)
//    }
//
//    val _: TypedField = resolver.includeTyped(TypedField(effectiveOrdinalField, ArrayType(IntegerType))).head
//
//    override lazy val repr(schema: SpookySchema[I]): SquashedRow :=> SquashedRow = Impl { v =>
//      val result = v.explodeData(onField, effectiveOrdinalField, forkType, sampler)
//      result
//    }
//  }

//  case class SaveContent[I](
//      getDocs: FetchedRow[I] => Map[String, Doc],
//      overwrite: Boolean
//  ) extends Delta[I, I] {
//
//    override def repr(schema: SpookySchema): SquashedRow[I] :=> SquashedRow[I] = {
//
//      :=> { v =>
//        val withCtx = v.withCtx(schema.ctx)
//
//        withCtx.unSquash
//          .foreach { fetchedRow =>
//            val docs = getDocs(fetchedRow)
//
//            docs.foreach {
//              case (k, doc) =>
//                doc.save(schema.ctx, overwrite).as(Seq(k))
//            }
//          }
//        v
//      }
//    }
//  }
}
