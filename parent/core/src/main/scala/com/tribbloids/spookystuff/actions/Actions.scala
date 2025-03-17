package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.HasTrace.MayChangeState

trait Actions extends HasTrace with MayChangeState {

  final override def exportNames: Set[String] = {
    val names = trace.map(_.exportNames)
    names.reduceLeftOption(_ ++ _).getOrElse(Set())
  }

  // names are not encoded in PageUID and are injected after being read from cache
  // TODO: remove, stateful update cannot be reasoned with
//  override def injectFrom(same: ActionLike): Unit = {
//    super.injectFrom(same)
//    val zipped = this.children.zip(same.asInstanceOf[Actions].children)
//
//    for (tuple <- zipped) {
//      tuple._1.injectFrom(tuple._2.asInstanceOf[tuple._1.type]) // recursive
//    }
//  }
}

object Actions {

  def empty: Nil.type = Nil
}
