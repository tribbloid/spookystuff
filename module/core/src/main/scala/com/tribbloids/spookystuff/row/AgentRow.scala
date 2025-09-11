package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.execution.{ExecutionContext, FlatMapPlan}
import com.tribbloids.spookystuff.row.AgentContext.Trajectory
import com.tribbloids.spookystuff.row.Data.ScopeRef

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object AgentRow {

  implicit class _seqView[D](self: Seq[AgentRow[D]]) {

    // TODO: fold into a SpookyDataset backed by a local/non-distributed data structure
    object flatMap {

      def apply[O: ClassTag](
          fn: FlatMapPlan.FlatMap._Fn[D, O]
      ): Seq[AgentRow[O]] = {

        self.flatMap { v =>
          val newData = FlatMapPlan.FlatMap.normalise(fn).apply(v)

          newData.map { datum =>
            v.copy(data = datum)
          }
        }
      }
    }
    def selectMany: flatMap.type = flatMap

    object map {

      def apply[O: ClassTag](fn: FlatMapPlan.Map._Fn[D, O]): Seq[AgentRow[O]] = {

        flatMap { row =>
          Seq(fn(row))
        }
      }
    }
    def select: map.type = map

  }

}

/**
  * abstracted data structure where expression can be resolved. not the main data structure in execution plan, Not
  * serializable, has to be created from [[SquashedRow]] on the fly
  */
case class AgentRow[D](
    localityGroup: LocalityGroup,
    data: D, // deliberately singular
    index: Int,
    ec: ExecutionContext
) {

  {
    agentContext.trajectoryBase // by definition, always pre-fetched to serve multiple data in a squashed row
  }

  @transient lazy val agentContext: AgentContext = {
    AgentContext.Static(localityGroup, ec)
    // will be discarded & recreated when being moved to another computer
    //  BUT NOT the already computed rollout trajectory!, they are carried within the localityGroup
  }

  @transient lazy val squash: SquashedRow[D] = SquashedRow(localityGroup, Seq(data -> index))

  @transient lazy val effectiveScope: ScopeRef = {

    val result = data match {
      case v: Data.Scoped[_] => v.scope
      case _                 =>
        val uids = agentContext.trajectoryBase.map(_.uid)
        ScopeRef(uids)
    }

    result
  }

  object rescope {

    // make sure no pages with identical name can appear in the same group.
    lazy val byDistinctNames: Seq[Data.Scoped[D]] = {
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

      effectiveScope.observationUIDs.foreach { uid =>
        if (innerBuffer.names.contains(uid.name)) {
          outerBuffer += innerBuffer.refs.toList
          innerBuffer.clear()
        }
        innerBuffer.add(uid)
      }
      outerBuffer += innerBuffer.refs.toList // always left, have at least 1 member

      outerBuffer.zipWithIndex.map {
        case (v, i) =>
          Data.Scoped(raw = data, scope = ScopeRef(v, i))
      }.toSeq
    }
  }

  lazy val trajectory: Trajectory[Observation] = {

    val seq = effectiveScope.observationUIDs.map { uid =>
      agentContext.trajectory.lookup(uid)
    }

    Trajectory(seq, agentContext)
  }

//  lazy val docs: Trajectory[Doc] = trajectory.docs
}
