package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.graph.Module
import com.tribbloids.spookystuff.utils.RangeArg

object FState {

  sealed trait Type extends Product

  case object Ordinary extends Type {

    override def toString: String = "---"
  }

  case object ROOT extends Type

  case object FINISH extends Type

  def isEnclosing(supr: RangeArg, sub: RangeArg): Boolean = {
    supr.start <= sub.start &&
    supr.end >= sub.end
  }

}

// compiled from a node
// may contain multiple rules that are sequentially matched against a stream
// first match will be served
case class FState(
    nodeView: FSMParserGraph.Layout.Core[Module[FSMParserGraph]]#NodeView
) {

  lazy val transitions: Seq[Transition] = nodeView.outbound2x.flatMap {
    case (edgeV, nodeV) =>
      val ruleOpt: Option[Rule] = edgeV.element.data
      ruleOpt.map { rule =>
        (rule, FState(nodeV))
      }
  }

  lazy val markers: List[Long] = {

    var proto = transitions.flatMap {
      case (rule, _) =>
        val window = rule.range
        val end = window.end + 1
        Seq(window.start, end)
    }
    proto = proto :+ 0L

    proto.toList.distinct.sortBy(_ - 1)
  }

  lazy val subRuleCache: Seq[(RangeArg, Transitions)] = {
    for (i <- 1 until markers.size) yield {
      val range: RangeArg = markers(i - 1) to (markers(i) - 1)

      val inRange = transitions.filter(v => FState.isEnclosing(v._1.range, range))
      range -> Transitions(inRange)
    }
  }
}
