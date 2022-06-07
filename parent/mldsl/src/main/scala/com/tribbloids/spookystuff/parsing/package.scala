package com.tribbloids.spookystuff

package object parsing {

  // TODO: add following optimisations into FSM compiler
  // - aggregate into Trie
  // - Aho-Corasick algorithm:
  // https://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_algorithm

  // not a finite state any more!
  type Phase = (FState, PhaseVec)

  type Rule = Pattern#Rule[Any]

  type Transition = (Rule, FState)
}
