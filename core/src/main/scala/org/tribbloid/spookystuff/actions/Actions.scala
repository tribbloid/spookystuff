package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.entity.PageRow
;

abstract class Actions(val self: Seq[Action]) extends ActionLike {

  final def outputNames = {
    val names = self.map(_.outputNames)
    if (names.nonEmpty) names.reduce(_ ++ _) //reduce cannot be applied on empty set
    else Set()
  }

  final protected def trunkSeq: Seq[Action] = self.flatMap(_.trunk)

  final protected def doInterpolateSeq(pr: PageRow): Seq[Action] = Actions.doInterppolateSeq(self, pr)

  //names are not encoded in PageUID and are injected after being read from cache
  override def injectFrom(same: this.type): Unit = {
    super.injectFrom(same)
    val zipped = this.self.zip(same.self)

    for (tuple <- zipped) {
      tuple._1.injectFrom(tuple._2.asInstanceOf[tuple._1.type ]) //recursive
    }
  }
}

object Actions {

  def doInterppolateSeq(self: Seq[Action], pr: PageRow): Seq[Action] = {
    val seq = self.map(_.doInterpolate(pr))

    if (seq.contains(None)) Seq()
    else seq.flatMap(option => option)
  }
}