package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.FState.SubRules
import com.tribbloids.spookystuff.parsing.Rule.Token

import scala.collection.mutable

case class BacktrackingManager(
    input: Seq[Token],
    initialState: StateWithMeta
) {

//  def maxBackTracking: Int = itr.maxBacktracking

  // before FState can be read from preceding Checkpoint in the stack or initialState
  // heavily stateful, backtracking only retreat to previous LinearSearch without recreating it.
  case class LinearSearch(
      previousState: StateWithMeta,
      start: Int = 0
      //      spanRange: Range, // refer to the buffer in BacktrackingIterator, the last Tokenacter is the matching token
      //      subRuleSearchStartFrom: Int = 0,
      //      gotos: Seq[Transition]
  ) {

    lazy val subRuleCache: Seq[(Range, FState.SubRules)] = previousState._1.subRuleCache

    var length: Int = 0 // strictly incremental
    var subRuleCacheII: Int = 0 // strictly incremental

    def end: Int = start + length
    def token: Token = input(end)

    // beforeFState == (length) ==> SubRules
    //        == (char) ==> Seq[Transition]
    //        == (index) ==> Transition ==> Rule, afterFState
    def getSubRules: SubRules = {

      while (subRuleCacheII < subRuleCache.length) {

        val hit = subRuleCache(subRuleCacheII)
        if (hit._1.contains(length)) return hit._2
        subRuleCacheII += 1
      }

      throw BacktrackingException(s"no rule is defined beyond length $length")
    }

    var transitionQueue: Seq[Transition] = Nil
    val precedingString: mutable.ArrayBuffer[Token] = mutable.ArrayBuffer.empty

    //update every state that depends on length
    def length_++(): Unit = {

      length += 1

      if (end >= input.length) throw BacktrackingException(s"reaching EOS at length $length")

      val subRules = getSubRules
      transitionQueue = subRules.transitionsMap.getOrElse(token, Nil)
      precedingString += input(end)
    }

    var transitionQueueII: Int = 0

    def nextTransition(): Transition = {

      transitionQueueII += 1

      while (transitionQueueII >= transitionQueue.length) {

        transitionQueueII = 0
        length_++()
      }

      transitionQueue(transitionQueueII)
    }

    var current: TransitionWithMeta = _

    def nextState(): StateWithMeta = {

      var transition: Transition = null
      var opt: Option[FStateMeta] = null

      do {
        transition = nextTransition()
        opt = transition._1.forward(precedingString, token, previousState._2)
      } while (opt.isEmpty)

      current = transition._1 -> (transition._2, opt.get)

      transition._2 -> opt.get
    }
  }

  val stack: mutable.Stack[LinearSearch] = mutable.Stack(LinearSearch(initialState))

  def advance(): Boolean = {
    val ls = stack.top // stack is from bottom to top

    try {
      val nextState = ls.nextState()

      nextState._1.nodeView.element.data match {
        case FState.Type.Normal =>
          val nextLS = LinearSearch(
            nextState,
            ls.length + 1
          )

          stack.push(nextLS)
          true
        case FState.Type.End =>
          false
      }

    } catch {
      case e: BacktrackingException =>
        stack.pop()
        true
    }
  }

  def untilTheEnd(): Unit = {

    while (advance()) {}
  }
}

object BacktrackingManager {}
