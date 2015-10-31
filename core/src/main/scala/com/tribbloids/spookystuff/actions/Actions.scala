package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.row.PageRow

abstract class Actions(val self: Trace) extends ActionLike {

  final def outputNames = {
    val names = self.map(_.outputNames)
    names.reduceLeftOption(_ ++ _).getOrElse(Set())
  }

  final protected def trunkSeq: Trace = self.flatMap(_.trunk)

  final protected def doInterpolateSeq(pr: PageRow, context: SpookyContext): Trace = Actions.doInterppolateSeq(self, pr, context: SpookyContext)

  //names are not encoded in PageUID and are injected after being read from cache
  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    val zipped = this.self.zip(same.asInstanceOf[Actions].self)

    for (tuple <- zipped) {
      tuple._1.injectFrom(tuple._2.asInstanceOf[tuple._1.type ]) //recursive
    }
  }
}

object Actions {

  def doInterppolateSeq(self: Trace, pr: PageRow, context: SpookyContext): Trace = {
    val seq = self.map(_.doInterpolate(pr, context))

    if (seq.contains(None)) Nil
    else seq.flatMap(option => option)
  }
}