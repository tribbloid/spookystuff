package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.entity.PageRow
;

abstract class Actions(val self: Seq[Action]) extends ActionLike {

  final def outputs = self.map(_.outputs).reduce(_ ++ _)

  final protected def trunkSeq: Seq[Action] = self.flatMap(_.trunk)

  //TODO: use inheritance and super functions
  final protected def doInterpolateSeq(pr: PageRow): Seq[Action] = Actions.doInterppolateSeq(self, pr)

  //names are not encoded in PageUID and are injected after being read from cache
  override def inject(same: this.type): Unit = {

    val zipped = this.self.zip(same.self)

    for (tuple <- zipped) {
      tuple._1.inject(tuple._2.asInstanceOf[tuple._1.type ]) //recursive
    }
  }
}

object Actions {

  def doInterppolateSeq(self: Seq[Action], pr: PageRow): Seq[Action] = { //TODO: use inheritance and super functions
  val seq = self.map(_.doInterpolate(pr))

    if (seq.contains(None)) Seq()
    else seq.flatMap(option => option)
  }
}