package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.entity.PageRow

abstract class Actions(val self: Seq[Action]) extends ActionLike {

  final def outputNames = {
    val names = self.map(_.outputNames)
    names.reduceLeftOption(_ ++ _).getOrElse(Set())
  }

  final protected def trunkSeq: Seq[Action] = self.flatMap(_.trunk)

  final protected def doInterpolateSeq(pr: PageRow): Seq[Action] = Actions.doInterppolateSeq(self, pr)

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

  def doInterppolateSeq(self: Seq[Action], pr: PageRow): Seq[Action] = {
    val seq = self.map(_.doInterpolate(pr))

    if (seq.contains(None)) Nil
    else seq.flatMap(option => option)
  }
}