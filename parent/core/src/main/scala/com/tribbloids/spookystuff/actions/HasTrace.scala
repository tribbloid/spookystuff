package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Foundation.HasTraceSet
import com.tribbloids.spookystuff.actions.HasTrace.StateChangeTag
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.{TreeView, Verbose}
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.relay.AutomaticRelay
import com.tribbloids.spookystuff.row.SpookySchema

import scala.language.implicitConversions

object HasTrace extends AutomaticRelay[HasTrace] {

  implicit def asTrace(v: HasTrace): Trace = v.trace

  // TODO: aggregate all object that has children
  case class TreeNodeView(
      actionLike: HasTrace
  ) extends TreeView.Immutable[TreeNodeView] {

    override def children: Seq[TreeNodeView] = actionLike.trace.map {
      TreeNodeView.apply
    }
  }

  sealed trait StateChangeTag extends HasTrace
  trait MayChangeState extends StateChangeTag {

    // the minimally required state changes that can be put into backtrace
    def stateChangeOnly: HasTrace
  }
  trait NoStateChange extends StateChangeTag
}

@SerialVersionUID(8566489926281786854L)
trait HasTrace extends HasTraceSet with Product with Serializable with Verbose {
  self: StateChangeTag =>

  def trace: Trace

  @transient final override lazy val traceSet: Set[Trace] = Set(trace)

  // many-to-one
  //  def +>(another: Action): Trace = Trace(asTrace :+ another)
  def +>(that: HasTrace): Trace = {

    //      (this, that) match {
    //        case (NoOp, _) => NoOp
    //        case (_, NoOp) => NoOp // TODO: should this be changed to EndOfStream?
    //        case _         => Trace(asTrace ++ that.asTrace)
    //      }

    Trace(trace ++ that.trace)
  }

//  lazy val TreeNode: ActionLike.TreeNodeView = ActionLike.TreeNodeView(this)

  /**
    * invoked on executors, immediately after definition *IMPORTANT!* may be called several times, before or after
    * Locality.
    */
  def dependentNormaliseRule[D](schema: SpookySchema): Seq[NormaliseRule[Trace]] = Nil
  // TODO: this needs to be tested BEFORE web module

//  def injectFrom(same: ActionLike): Unit = {}
  // TODO: remove, use Ser/De deep copy in case all variables needs to be copied

  // used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def hasExport: Boolean = exportNames.nonEmpty

  def exportNames: Set[String] = Set.empty

  def apply(agent: Agent): Seq[Observation]

}
