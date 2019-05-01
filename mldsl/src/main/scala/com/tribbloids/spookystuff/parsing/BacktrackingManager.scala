package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.Exceptions.{BacktrackingFailure, ParsingError}
import com.tribbloids.spookystuff.parsing.FState.SubRules
import com.tribbloids.spookystuff.parsing.Pattern.Token
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

    val subRuleCache: Seq[(RangeArg, FState.SubRules)] = prevPhase._1.subRuleCache

    var _length: Long = 0L // strictly incremental
    var subRuleCacheII: Int = 0 // strictly incremental

    val spanTokens: mutable.ArrayBuffer[Token] = mutable.ArrayBuffer()

    var transitionQueue: Seq[Rule_FState] = Nil

    var transitionQueueII: Int = 0

    //init
    {
      updateState()
    }

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
        throw BacktrackingFailure(s"reaching EOS at length ${_length}")

      _length += 1

      updateState()
    }

    def length_+=(v: Int): Unit = {
      for (i <- 0 until v) length_++() //TODO: inefficient! only the last update require refreshing transitionQueue
    }

    def findValidState(): Rule_FState = {

      while (transitionQueueII >= transitionQueue.length) {

        length_++()
      }
      transitionQueue(transitionQueueII)
    }

    // beforeFState == (length) ==> SubRules
    //        == (char) ==> Seq[Transition]
    //        == (index) ==> Transition ==> Rule, afterFState
    def getSubRules: SubRules = {

      while (subRuleCacheII < subRuleCache.length) {

        val hit = subRuleCache(subRuleCacheII)
        if (hit._1.delegate.containsLong(_length)) return hit._2
        subRuleCacheII += 1
      }

      throw BacktrackingFailure(s"no rule is defined beyond length ${_length}")
    }

    var currentOutcome: (Rule, Outcome[Any]) = _

    def nextPhase: Phase = {

      while (true) {

        val transition = findValidState()
        val nextResult = transition._1.resultFn(spanTokens, prevPhase)

        nextResult.nextPhaseVecOpt match {
          case PhaseVec.NoOp(skipOpt) =>
            skipOpt match {
              case Some(skip) => length_+=(skip + 1)
              case None       => transitionQueueII += 1
            }
          case _ =>
            transitionQueueII += 1
            currentOutcome = transition._1 -> nextResult
            return transition._2 -> nextResult.nextPhaseVecOpt.asInstanceOf[PhaseVec]
        }
      }

      throw new UnsupportedOperationException("IMPOSSIBLE!")
    }
  }

  val stack: mutable.Stack[LinearSearch] = mutable.Stack(LinearSearch(initialState))

  def advance(): Boolean = {
    if (stack.isEmpty)
      throw ParsingError(s"cannot parse '${input.mkString}'")

    val onTop = stack.top // stack is from bottom to top

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

          stack.push(nextLS)
          true
      }

    } catch {
      case e: BacktrackingFailure =>
        val ee = e
        stack.pop()
        true
    }
  }

  def run_!(): Unit = {

    while (advance()) {}
  }
}

object BacktrackingManager {}
