package com.tribbloids.spookystuff.actions

trait Actions extends ActionLike {

  final override def outputNames: Set[String] = {
    val names = children.map(_.outputNames)
    names.reduceLeftOption(_ ++ _).getOrElse(Set())
  }

  final protected def childrenSkeleton: Trace = children.flatMap(_.skeleton)

  // names are not encoded in PageUID and are injected after being read from cache
  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    val zipped = this.children.zip(same.asInstanceOf[Actions].children)

    for (tuple <- zipped) {
      tuple._1.injectFrom(tuple._2.asInstanceOf[tuple._1.type]) // recursive
    }
  }
}

object Actions {

  def empty: Nil.type = Nil
}
