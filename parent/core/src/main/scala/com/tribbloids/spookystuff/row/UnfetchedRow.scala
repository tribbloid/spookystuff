//package com.tribbloids.spookystuff.row
//
//import ai.acyclic.prover.commons.function.PreDef
//import ai.acyclic.prover.commons.function.PreDef.:=>
//import com.tribbloids.spookystuff.{CanRunWithCtx, SpookyContext}
//import com.tribbloids.spookystuff.actions.{Trace, TraceSet}
//import com.tribbloids.spookystuff.doc.{Observation, ScopeRef}
//import com.tribbloids.spookystuff.dsl.ForkType
//import com.tribbloids.spookystuff.extractors.Resolved
//
//import java.util.UUID
//import scala.language.implicitConversions
//
//object SquashedRow {
//
//  case class SrcRow(
//      group: LocalityGroup = LocalityGroup.NoOp,
//      dataRows: Vector[DataRow] = Vector()
//  ) extends SquashedRow {}
//
//  case class DeltaRow(
//      prev: SquashedRow,
//      delta: SquashedRow :=> Seq[SquashedRow]
//  ) extends SquashedRow
//
//  def apply(data: Map[Field, Any]): SquashedRow = SrcRow(
//    dataRows = Vector(DataRow(data))
//  )
//
//  lazy val empty: SquashedRow = apply(Map.empty[Field, Any])
//
//  /**
//    * @param schema
//    *   CAUTION! this is the schema after all delta has been applied it will not match dataRos in raw
//    */
//  case class WithSchema(
//      row: SquashedRow,
//      schema: SpookySchema
//  ) {
//
//    lazy val withCtx: row._WithCtx = row.withCtx(schema.spooky)
//
//  }
//
//  object WithSchema {
//
//    implicit def asWithCtx(
//        v: WithSchema
//    ): v.row._WithCtx = v.withCtx
//  }
//}
//
///**
//  * the main data structure in execution plan, chosen for its very small serialization footprint (a.k.a. "Bottleneck")
//  *
//  * consisting of several dataRows from the previous stage, a shared traceView, and a function representing all agent
//  * actions and data transformations, Observations are deliberately omitted for being too large and will slow down
//  * shipping
//  *
//  * for access concrete data & observations, use SquashedRow or FetchedRow
//  */
//sealed trait SquashedRow extends CanRunWithCtx {
//
//  import SquashedRow._
//
//  @transient lazy val src: SrcRow = this match {
//    case v: SrcRow   => v
//    case v: DeltaRow => v.prev.src
//  }
//
//  def setCache(
//      vs: Seq[Observation] = null
//  ): this.type = {
//    this.src.group.trace.cached(vs)
//    this
//  }
//
//  private object AndThen {
//
//    def flatMap(next: SquashedRow :=> Seq[SquashedRow]): DeltaRow = {
//
//      DeltaRow(
//        SquashedRow.this,
//        next
//      )
//
//    }
//
//    def map(next: SquashedRow :=> SquashedRow): DeltaRow = {
//
//      flatMap(
//        next.andThen { v: SquashedRow =>
//          Seq(v)
//        }
//      )
//    }
//  }
//
//  // cache it
//  case class _WithCtx private (
//      spooky: SpookyContext
//  ) {
//
//    lazy val squashedRows: Seq[SquashedRow] = {
//
//      SquashedRow.this match {
//        case v: SrcRow =>
//          val dataRowsWithLineageIDs = v.dataRows.map { row =>
//            row.copy(
//              exploreLineageID = Some(UUID.randomUUID())
//            )
//          }
//
//          lazy val fetched = v.group.trace.WithCtx(spooky).value
//
//          val result = SquashedRow(dataRowsWithLineageIDs, ScopeRef.raw(fetched))
//          Seq(result)
//        case v: DeltaRow =>
//          v.prev.withCtx(spooky).squashedRows.flatMap { row =>
//            val result = v.delta(row)
//            result
//          }
//      }
//    }
//
//    lazy val fetchedRows: Seq[FetchedRow] = squashedRows.flatMap(_.unSquash)
//
//    lazy val dataRows: Seq[DataRow] = squashedRows.flatMap(_.dataRows)
//  }
//
//}
