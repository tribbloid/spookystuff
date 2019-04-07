package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Rules.SubRules
import com.tribbloids.spookystuff.utils.MultiMapView

import scala.annotation.tailrec

object Rules {

  def isEnclosing(supr: Range, sub: Range) = {
    supr.end >= sub.end
  }

  case class SubRules(vs: Seq[(Rule, Rules)]) {

    lazy val kvs: Seq[(Char, (Rule, Rules))] = vs.map { v =>
      v._1.token -> v
    }

    lazy val mapView: MultiMapView[Char, (Rule, Rules)] = {

      MultiMapView.Immutable.apply(kvs: _*)
    }
  }
}

// compiled from a node
case class Rules(
    nodeView: ParserGraph.DSL.Core#NodeView
) {

  lazy val vs: Seq[(Rule, Rules)] = nodeView.outbound2x.flatMap {
    case (edgeV, nodeV) =>
      val ruleOpt: Option[Rule] = edgeV.element.data
      ruleOpt.map { rule =>
        rule -> Rules(nodeV)
      }
  }

  lazy val markers: List[Int] = {

    var proto = vs.flatMap {
      case (rule, _) =>
        val window = rule.range
        Seq(window.start, window.end + 1)
    }
    proto :+= 0

    proto.toList.distinct.sorted
  }

  lazy val subRuleCache: Seq[(Range, SubRules)] = {
    for (i <- 1 until markers.size) yield {

      val range = markers(i - 1) until markers(i)

      val inRange = vs.filter(v => Rules.isEnclosing(v._1.range, range))
      range -> SubRules(inRange)
    }
  }

}
