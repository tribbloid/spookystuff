package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.FState.SubRules
import com.tribbloids.spookystuff.parsing.Rule.Token
import com.tribbloids.spookystuff.utils.{MultiMapView, RangeArg}

import scala.collection.immutable.NumericRange

object FState {

  sealed trait Type extends Product

  case object Ordinary extends Type {

    override def toString: String = "---"
  }

  case object START extends Type

  case object FINISH extends Type

  def isEnclosing(supr: RangeArg, sub: RangeArg): Boolean = {
    supr.start <= sub.start &&
    supr.end >= sub.end
  }

  case class SubRules(vs: Seq[Transition]) {

    val kvs: Seq[(Token, Transition)] = vs.map { v =>
      v._1.token -> v
    }

    // Not the fastest, Charset doesn't grow dynamically
    val transitionsMap: MultiMapView[Token, Transition] = {

      MultiMapView.Immutable.apply(kvs: _*)
    }
  }
}

// compiled from a node
// may contain multiple rules that are sequentially matched against a stream
// first match will be served
case class FState(
    nodeView: FSMParserGraph.Layout.Core#NodeView
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
        Seq(window.start, window.end + 1)
    }
    proto = proto :+ 0L

    proto.toList.distinct.sorted
  }

  lazy val subRuleCache: Seq[(RangeArg, SubRules)] = {
    for (i <- 1 until markers.size) yield {
      val _range: NumericRange[Long] = markers(i - 1) until markers(i)
      val range: RangeArg = RangeArg.fromNumericRange(_range)

      val inRange = transitions.filter(v => FState.isEnclosing(v._1.range, range))
      range -> SubRules(inRange)
    }
  }
}
