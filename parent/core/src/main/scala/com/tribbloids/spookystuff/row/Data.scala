package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.doc.Observation.DocUID

import java.util.UUID
import scala.language.implicitConversions

trait Data[D] {
  // TODO: merge with BuildRow, move into object

  def raw: D
//  def sourceScope: Option[SourceScope]
}

object Data {

  implicit def unbox[D](v: Data[D]): D = v.raw

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

  case class Scope(
      observationUIDs: Seq[DocUID],
      ordinalIndex: Int = 0

      // a list of DocUIDs that can be found in associated Rollout, DocUID has small serialized form
  ) {}

  object Scoped {

    def unscoped[D](data: D): Scoped[D] = Scoped(data, Scope(Nil))

//    def default[D](data: D): Scoped[D] = Scoped(data, None)

  }

  // TODO: can this be a dependent type since scopeUIDs has to be tied to a Rollout?
  case class Scoped[D](
      raw: D,
      scope: Scope
      // if missing, always refers to all snapshoted observations in the trajectory
  ) extends Data[D] {}

  object Exploring {}

  // TODO:  case class Fetching

  /**
    * contains all schematic data accumulated over graph traversal path, but contains no schema, ad-hoc local access
    * requires combining with schema from Spark driver
    *
    * CAUTION: implementation should be simple and close to DataSet API in Apache Spark, all type-level operations
    * should go into [[com.tribbloids.spookystuff.linq.LinqBase]]
    *
    * @param raw
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
      raw: D,
      lineageID: Option[UUID] = None,
      isOutOfRange: Boolean = false,
      depthOpt: Option[Int] = None,
      path: Vector[Scope] = Vector.empty
      // contain every used sourceScope in exploration history
  ) extends Data[D] {

    //  {
    //    assert(data.isInstanceOf[Serializable]) // fail early  TODO: this should be moved into Debugging mode
    //  }

    def idRefresh: Exploring[D] = this.copy(lineageID = Some(UUID.randomUUID()))

    def depth_++ : Exploring[D] = this.copy(depthOpt = Some(depthOpt.map(_ + 1).getOrElse(0)))

    lazy val orderBy: (Option[Int], Vector[Int]) = (depthOpt, path.map(_.ordinalIndex))
  }
}
