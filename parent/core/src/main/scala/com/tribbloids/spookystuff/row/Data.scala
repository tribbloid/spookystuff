package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.doc.Observation.DocUID

import java.util.UUID

trait Data[+D] {
  // TODO: merge with BuildRow, move into object

  def raw: D
//  def sourceScope: Option[SourceScope]
}

object Data {

  case class Scope(
      observationUIDs: Seq[DocUID],
      index: Int = 0

      // a list of DocUIDs that can be found in associated Rollout, DocUID has small serialized form
  ) {}

  object Scoped {

    def unscoped[D](data: D): Scoped[D] = Scoped(data, Scope(Nil))
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
  case class Exploring[+D](
      raw: D,
      lineageID: Option[UUID] = None, // should be non-optional
      isOutOfRange: Boolean = false,
      depth: Int = 0
//      path: Vector[Scope] = Vector.empty
      // contain every used sourceScope in exploration history
  ) extends Data[D] {

    //  {
    //    assert(data.isInstanceOf[Serializable]) // fail early  TODO: this should be moved into Debugging mode
    //  }

    def startLineage: Exploring[D] = this.copy(lineageID = Some(UUID.randomUUID()))

    def depth_++ : Exploring[D] = this.copy(depth = depth + 1)

//    lazy val orderBy: (Int, Vector[Int]) = (depth, path.map(_.index))
  }
}
