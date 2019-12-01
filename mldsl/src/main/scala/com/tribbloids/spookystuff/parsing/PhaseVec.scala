package com.tribbloids.spookystuff.parsing

// TODO: generalise!
trait PhaseVec

object PhaseVec {

  case object Eye extends PhaseVec

  case object NoOp extends PhaseVec

  case class Skip(length: Int) extends PhaseVec {

    //    def next(bm: BacktrackingManager#LinearSearch): Option[Like] = {
    //
    //      skipOpt match {
    //        case Some(skip) => bm.length_+=(skip + 1)
    //        case None       => bm.transitionQueueII += 1 //TODO: is it really useful?
    //      }
    //      None
    //    }
  }

  case class Depth(v: Int) extends PhaseVec {}

  //  trait Transition extends Like {
  //
  //    def next(bm: BacktrackingManager#LinearSearch): Option[Like] = {
  //
  //      bm.transitionQueueII += 1
  //      bm.currentOutcome = transition._1 -> nextResult
  //      return transition._2 -> nextResult.nextPhaseVecOpt.asInstanceOf[PhaseVec]
  //    }
  //
  //  }
}
