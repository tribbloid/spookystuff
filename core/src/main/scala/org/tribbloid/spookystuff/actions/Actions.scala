package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.entity.PageRow
;

abstract class Actions(val self: Seq[Action]) extends ActionLike {

  assert(self.nonEmpty)

  final def outputs = self.map(_.outputs).reduce(_ ++ _)

  final protected def trunkSeq: Seq[Action] = self.flatMap {
    _.trunk
  }

  final protected def doInterpolateSeq(pr: PageRow): Seq[Action] = {
    //TODO: use inheritance and super functions
    val seq = self.map(_.doInterpolate(pr))

    if (seq.contains(null)) null
    else seq.map(action => action)
  }

  //names are not encoded in PageUID and are injected after being read from cache
  override def inject(same: this.type): Unit = {

    //    super.inject(same)

    assert(this.self.size == same.self.size)
    val zipped = this.self.zip(same.self)

    for (tuple <- zipped) {
      tuple._1.inject(tuple._2.asInstanceOf[tuple._1.type ]) //recursive
    }
  }
}