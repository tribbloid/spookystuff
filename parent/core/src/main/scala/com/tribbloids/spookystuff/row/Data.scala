package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.doc.Observation.DocUID

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object Data {

  object WithScope {

    implicit def unbox[D <: Serializable](v: WithScope[D]): D = v.data

    def empty[D](data: D): WithScope[D] = WithScope(data, Nil)

    lazy val blank: WithScope[Unit] = empty(())
  }

  case class WithScope[D](
      data: D,
      scopeUIDs: Seq[DocUID],
      // a list of DocUIDs that can be found in associated Rollout, DocUID has very small serialized form
      ordinal: Int = 0
  ) {

    // make sure no pages with identical name can appear in the same group.
    lazy val splitByDistinctNames: Seq[WithScope[D]] = {
      val outerBuffer: ArrayBuffer[Seq[DocUID]] = ArrayBuffer()

      object innerBuffer {
        val refs: mutable.ArrayBuffer[DocUID] = ArrayBuffer()
        val names: mutable.HashSet[String] = mutable.HashSet[String]()

        def add(uid: DocUID): Unit = {
          refs += uid
          names += uid.name
        }

        def clear(): Unit = {
          refs.clear()
          names.clear()
        }
      }

      scopeUIDs.foreach { uid =>
        if (innerBuffer.names.contains(uid.name)) {
          outerBuffer += innerBuffer.refs.toList
          innerBuffer.clear()
        }
        innerBuffer.add(uid)
      }
      outerBuffer += innerBuffer.refs.toList // always left, have at least 1 member

      outerBuffer.zipWithIndex.map {
        case (v, i) =>
          this.copy(scopeUIDs = v, ordinal = i)
      }.toSeq
    }
  }

  object Exploring {}

  /**
    * contains all schematic data accumulated over graph traversal path, but contains no schema, ad-hoc local access
    * requires combining with schema from Spark driver
    *
    * CAUTION: implementation should be simple and close to DataSet API in Apache Spark, all type-level operations
    * should go into [[com.tribbloids.spookystuff.frameless.TypedRow]]
    *
    * @param data
    *   internal representation
    * @param lineageID
    *   only used in [[com.tribbloids.spookystuff.dsl.PathPlanning]], multiple [[Exploring]] with identical
    *   [[lineageID]] are assumed to be scrapped from the same graph traversal path, and are preserved or removed in
    *   [[com.tribbloids.spookystuff.dsl.PathPlanning]] as a unit
    * @param isOutOfRange
    *   only used in [[com.tribbloids.spookystuff.execution.ExploreRunner]], out of range rows are cleaned at the end of
    *   all [[com.tribbloids.spookystuff.execution.ExplorePlan]] minibatches
    */
  @SerialVersionUID(6534469387269426194L)
  case class Exploring[D](
      data: D,
      lineageID: Option[UUID] = None,
      isOutOfRange: Boolean = false,
      depthOpt: Option[Int] = None,
      ordinal: Vector[Int] = Vector.empty
  ) {

    //  {
    //    assert(data.isInstanceOf[Serializable]) // fail early  TODO: this should be moved into Debugging mode
    //  }

    def idRefresh: Exploring[D] = this.copy(lineageID = Some(UUID.randomUUID()))

    def depth_++ : Exploring[D] = this.copy(depthOpt = Some(depthOpt.map(_ + 1).getOrElse(0)))

    lazy val sortEv: (Option[Int], Vector[Int]) = (depthOpt, ordinal)
  }

  case class Forking[D, K](
      data: D,
      // the following are temporary columns used in forking, can be committed into D on demand
      // they will be missing if it is a outer fork that failed to produce any result
      key: Option[K],
      ordinal: Option[Int] // will be appended into Exploring.ordinal by default
  ) {}
}
