package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}

abstract class Actions(override val children: Trace) extends ActionLike {

  override final def outputNames = {
    val names = children.map(_.outputNames)
    names.reduceLeftOption(_ ++ _).getOrElse(Set())
  }

  final protected def trunkSeq: Trace = children.flatMap(_.skeleton)

  final protected def doInterpolateSeq(pr: FetchedRow, schema: SpookySchema): Trace =
    Actions.doInterppolateSeq(children, pr, schema: SpookySchema)

  //names are not encoded in PageUID and are injected after being read from cache
  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    val zipped = this.children.zip(same.asInstanceOf[Actions].children)

    for (tuple <- zipped) {
      tuple._1.injectFrom(tuple._2.asInstanceOf[tuple._1.type ]) //recursive
    }
  }
}

object Actions {

  def doInterppolateSeq(self: Trace, pr: FetchedRow, schema: SpookySchema): Trace = {
    val seq = self.map(_.doInterpolate(pr, schema))

    if (seq.contains(None)) Nil
    else seq.flatten
  }

  def empty = Nil
}