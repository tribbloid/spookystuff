package com.tribbloids.spookystuff.parsing

// TODO: make more stateful? maybe it doesn't matter after all
case class FStateMeta(
    skip: Int = 0,
    depth: Int = 0
    // if true, immediately output the result, ignore NodeState transition
//    endOfParsing: Boolean = false
) {}
