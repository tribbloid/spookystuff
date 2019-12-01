package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Pattern.Token
import com.tribbloids.spookystuff.parsing.exception.{BacktrackableFailure, BacktrackableMixin, ParsingError}
import com.tribbloids.spookystuff.utils.RangeArg

import scala.collection.mutable

case class BacktrackingManager(
    input: Seq[Token],
    initialState: Phase
) {

//  def maxBackTracking: Int = itr.maxBacktracking

  // before FState can be read from preceding Checkpoint in the stack or initialState
  // heavily stateful, backtracking only retreat to previous LinearSearch without recreating it.
  case class LinearSearch(
      prevPhase: Phase,
      start: Long = 0L
      //      spanRange: Range, // refer to the buffer in BacktrackingIterator, the last Tokenacter is the matching token
      //      subRuleSearchStartFrom: Int = 0,
      //      gotos: Seq[Transition]
  ) {

    val subRuleCache: Seq[(RangeArg, Transitions)] = prevPhase._1.subRuleCache

    var _length: Long = 0L // strictly incremental
    var subRuleCacheII: Int = 0 // strictly incremental

    val spanTokens: mutable.ArrayBuffer[Token] = {
      val result = new mutable.ArrayBuffer[Token](2048)
      result
    }

    var transitionQueue: Seq[Transition] = Nil

    var transitionQueueII: Int = 0

    //init
    {
      updateState()
    }

    //TODO: these are too slow and don't support streaming
    def end(length: Long = this._length): Long = start + length
    def token: Token = input(end().toInt)

    private def updateState(): Unit = {
      val subRules = getSubRules

      transitionQueue = subRules.transitionsMap.getOrElse(token, Nil)

      spanTokens += token
      transitionQueueII = 0
    }

    //update every state that depends on length
    def length_++(): Unit = {

      if (end(_length + 1) >= input.length)
        throw BacktrackableFailure(s"reaching EOS at length ${_length}")

      _length += 1

      updateState()
    }

    def length_+=(v: Int): Unit = {
      for (_ <- 0 until v) length_++() //TODO: inefficient! only the last update require refreshing transitionQueue
    }

    def findValidTransition(): Transition = {

      while (transitionQueueII >= transitionQueue.length) {

        length_++()
      }
      transitionQueue(transitionQueueII)
    }

    // beforeFState == (length) ==> SubRules
    //        == (char) ==> Seq[Transition]
    //        == (index) ==> Transition ==> Rule, afterFState
    def getSubRules: Transitions = {

      while (subRuleCacheII < subRuleCache.length) {

        val hit = subRuleCache(subRuleCacheII)
        if (hit._1.delegate.containsLong(_length)) return hit._2
        subRuleCacheII += 1
      }

      throw BacktrackableFailure(s"no rule is defined beyond length ${_length}")
    }

    var currentOutcome: (Rule, Outcome[Any]) = _

    def nextPhase: Phase = {

      while (true) {

        val transition: Transition = findValidTransition()
        val outcome: Outcome[Any] = transition._1.fn(spanTokens, prevPhase)

        val nextPhaseVec = outcome.nextPhaseVec

        nextPhaseVec match {
          case PhaseVec.NoOp(skipOpt) =>
            skipOpt match {
              case Some(skip) => length_+=(skip + 1)
              case None       => transitionQueueII += 1
            }
          case _ =>
            transitionQueueII += 1
            currentOutcome = transition._1 -> outcome
            return transition._2 -> nextPhaseVec
        }
      }

      throw new UnsupportedOperationException("IMPOSSIBLE!")
    }
  }

  val stack: mutable.ArrayBuffer[LinearSearch] = mutable.ArrayBuffer(LinearSearch(initialState))

  def advance(): Boolean = {
    if (stack.isEmpty)
      throw ParsingError(
        s"""cannot parse '${input.mkString}'
           |all parsing rules are not applicable after backtracking
           |""".stripMargin
      )

    val onTop = stack.last // stack is from bottom to top

    try {
      val nextPhase = onTop.nextPhase

      nextPhase._1.nodeView.element.data match {

        case FState.FINISH =>
          false

        case _ =>
          val nextLS = LinearSearch(
            nextPhase,
            onTop.end() + 1L
          )

          stack += nextLS
          true
      }

    } catch {
      case _: BacktrackableMixin =>
        stack.remove(stack.length - 1)
        true
    }
  }

  def run_!(): Unit = {

    while ({
      val hasMore = advance()
      hasMore
    }) {}
  }
}

object BacktrackingManager {}
