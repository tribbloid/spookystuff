package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.doc.Observation.DocUID

import java.util.UUID
import scala.language.implicitConversions

trait Data[D] {

  def data: D
}

object Data {

//  case class Raw[D](
//      data: D
//  ) extends Data[D]
//
//  case class Flatten[D]( TODO: should I use this?
//      data: D,
  //      // temporary columns used in inner/outer join, can be committed into D on demand
  //      // they will be missing if it is a outer fork that failed to produce any result
//      key: Option[D],
//      ordinalIndex: Int = 0
//  ) extends Data[D]

  case class SourceScope(
      observations: Seq[DocUID] = Nil,
      ordinalIndex: Int = 0

      // a list of DocUIDs that can be found in associated Rollout, DocUID has small serialized form
  ) {}

  object WithScope {

    implicit def unbox[D <: Serializable](v: WithScope[D]): D = v.data

    def unscoped[D](data: D): WithScope[D] = WithScope(data, Some(SourceScope(Nil)))

    def default[D](data: D): WithScope[D] = WithScope(data, None)

    lazy val ofUnit: WithScope[Unit] = default(())
  }

  // TODO: can this be a dependent type since scopeUIDs has to be tied to a Rollout?
  case class WithScope[D](
      data: D,
      sourceScope: Option[SourceScope] = None
      // if missing, always refers to all snapshoted observations in the trajectory
  ) extends Data[D] {}

  object Exploring {}

  /**
    * contains all schematic data accumulated over graph traversal path, but contains no schema, ad-hoc local access
    * requires combining with schema from Spark driver
    *
    * CAUTION: implementation should be simple and close to DataSet API in Apache Spark, all type-level operations
    * should go into [[com.tribbloids.spookystuff.linq.LinqBase]]
    *
    * @param data
    *   internal representation
    * @param lineageID
    *   only used in [[com.tribbloids.spookystuff.dsl.PathPlanning]], multiple [[Exploring]] with identical
    *   [[lineageID]] are assumed to be scrapped from the same graph traversal path, and are preserved or removed in
    *   [[com.tribbloids.spookystuff.dsl.PathPlanning]] as a unit
    * @param isOutOfRange
    *   only used in [[com.tribbloids.spookystuff.execution.ExploreRunner]], out of range rows are cleaned at the end of
    *   all [[com.tribbloids.spookystuff.execution.ExplorePlan]] epochs
    */
  @SerialVersionUID(6534469387269426194L)
  case class Exploring[D](
      payload: WithScope[D],
      lineageID: Option[UUID] = None,
      isOutOfRange: Boolean = false,
      depthOpt: Option[Int] = None,
      path: Vector[SourceScope] = Vector.empty
      // contain every used sourceScope in exploration history
  ) extends Data[D] {

    override def data: D = payload.data
    //  {
    //    assert(data.isInstanceOf[Serializable]) // fail early  TODO: this should be moved into Debugging mode
    //  }

    def idRefresh: Exploring[D] = this.copy(lineageID = Some(UUID.randomUUID()))

    def depth_++ : Exploring[D] = this.copy(depthOpt = Some(depthOpt.map(_ + 1).getOrElse(0)))

    lazy val orderBy: (Option[Int], Vector[Int]) = (depthOpt, path.map(_.ordinalIndex))
  }
}
