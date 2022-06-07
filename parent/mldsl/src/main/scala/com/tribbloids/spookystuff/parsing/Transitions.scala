package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Pattern.Token
import com.tribbloids.spookystuff.utils.MultiMapView

case class Transitions(vs: Seq[Transition]) {

  val kvs: Seq[(Token, Transition)] = vs.map { v =>
    v._1.token -> v
  }

  // Not the fastest, Charset doesn't grow dynamically
  val transitionsMap: MultiMapView[Token, Transition] = {

    MultiMapView.Immutable.apply(kvs: _*)
  }
}
