package com.tribbloids.spookystuff.parsing

// TODO: generalise!
case class PhaseVec(
    depth: Int = 0
) extends PhaseVec.Like

object PhaseVec {

  trait Like

  case class NoOp(
      skipOpt: Option[Int] = None
  ) extends Like
}
