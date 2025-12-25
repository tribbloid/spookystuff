package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.Verbose
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.relay.AutomaticRelay

import scala.language.implicitConversions

object HasTrace extends AutomaticRelay[HasTrace] {

  implicit def unbox(v: HasTrace): Trace = v.trace
}

@SerialVersionUID(8566489926281786854L)
trait HasTrace extends HasTraceSet with Product with Serializable with Verbose {

  def trace: Trace

  val isStateful: Boolean = true
  val isDeteriministic: Boolean = true

  /**
    * 4 combinations:
    *   - stateless, deterministic (pure): result can be cached by the invocation along
    *   - stateful, deterministic: result can be cached by [[com.tribbloids.spookystuff.doc.Observation.ReplayUID]]
    *   - stateless, non-deterministic: result should not be cached
    *   - stateful, non-deterministic: all results from it and other invocations executed after on the same harness also
    *     should not be cached, most viral
    * they are usually variables but can also be constants at type-level, in which case they can participate in static
    * verification & optimisation
    */
  def stateChangeOnly: HasTrace = if (isStateful) this else NoOp

  @transient final override lazy val traceSet: Set[Trace] = Set(trace)

  object append {

    // many-to-one
    //  def +>(another: Action): Trace = Trace(asTrace :+ another)
    def apply(that: HasTrace): Trace = {

      //      (this, that) match {
      //        case (NoOp, _) => NoOp
      //        case (_, NoOp) => NoOp // TODO: should this be changed to EndOfStream?
      //        case _         => Trace(asTrace ++ that.asTrace)
      //      }

      Trace(trace ++ that.trace)
    }
  }

  def +> : append.type = append

  // used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def hasExport: Boolean = exportNames.nonEmpty

  def exportNames: Set[String] = Set.empty

  def apply(agent: Agent): Seq[Observation]
}
