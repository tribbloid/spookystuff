package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Pattern.Token
import com.tribbloids.spookystuff.commons.collection.MultiMapOps

case class Transitions(vs: Seq[Transition]) {

  val kvs: Seq[(Token, Transition)] = vs.map { v =>
    v._1.token -> v
  }

  // Not the fastest, Charset doesn't grow dynamically
  val transitionsMap: MultiMapOps[Token, Transition] = {

    MultiMapOps.Immutable.apply(kvs: _*)
  }
}
