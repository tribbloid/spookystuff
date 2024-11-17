package com.tribbloids.spookystuff.parsing

import ai.acyclic.prover.commons.collection.MultiMaps
import com.tribbloids.spookystuff.parsing.Pattern.Token

case class Transitions(vs: Seq[Transition]) {

  val kvs: Seq[(Token, Transition)] = vs.map { v =>
    v._1.token -> v
  }

  // Not the fastest, Charset doesn't grow dynamically
  val transitionsMap: MultiMaps.Immutable[Token, Transition] = {

    MultiMaps.Immutable.apply(kvs*)
  }
}
