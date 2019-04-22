package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Layout.Facet
import com.tribbloids.spookystuff.graph.Module.Heads

trait FlowLayout[D <: Domain] extends Layout[D] {

  import FlowLayout._

  lazy val facets = List(FromLeft, FromRight)

  trait DSLLike extends super.DSLLike {

    lazy val `:>>` = core.Ops(FromLeft, FromRight)
    lazy val `<<:` = core.Ops(FromRight, FromLeft)

    // select from

    def from(filter: EdgeFilter[D]): Self = {
      val newFrom = Heads[D](filter(core._graph))
      core.copy(
        fromOverride = Some(newFrom)
      )
    }

    //TODO: these symbols are lame
    def >-(filter: EdgeFilter[D]): Self = from(filter)

    def and(filter: EdgeFilter[D]) = {
      val newFrom = Heads[D](filter(core._graph))
      val mergedFrom = Heads[D](core.from.seq ++ newFrom.seq)
      core.copy(
        fromOverride = Some(mergedFrom)
      )
    }
    def <>-(filter: EdgeFilter[D]): Self = and(filter)

    // undirected

    def union(another: DSLLike): Self = {
      `:>>`.union(another.core)
    }
    def U(another: DSLLike) = union(another)
    def ||(another: DSLLike) = union(another)

    // left > right

    def merge_>(right: DSLLike): Self = `:>>`.merge(right.core)
//    def merge(right: Interface) = merge_>(right)
    def >>>(right: DSLLike) = merge_>(right)
    def >(right: DSLLike) = merge_>(right)

    //TODO: fast-forward handling: if right is reused for many times,
    // ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
    def rebase_>(right: DSLLike): Self = `:>>`.rebase(right.core)
    def rebase(right: DSLLike) = rebase_>(right)
    def >=>(right: DSLLike) = rebase_>(right)

    //this is really kind of ambiguous
    def commit_>(right: DSLLike): Self = `:>>`.commit(right.core)
    def commit(right: DSLLike) = commit_>(right)
    def >->(right: DSLLike) = commit_>(right)

    // left < right
    //TODO: follow :>> & <<: convention

    def merge_<(left: DSLLike): Self = `<<:`.merge(left.core)
    def egrem(prev: DSLLike) = prev.merge_<(this)
    def <<<(prev: DSLLike) = prev.merge_<(this)
    def <(prev: DSLLike) = prev.merge_<(this)

    def rebase_<(left: DSLLike): Self = `<<:`.rebase(left.core)
    def esaber(prev: DSLLike) = prev.rebase_<(this)
    def <=<(prev: DSLLike) = prev.rebase_<(this)

    def commit_<(left: DSLLike): Self = `<<:`.commit(left.core)
    def timmoc(left: DSLLike) = left.commit_<(this)
    def <-<(left: DSLLike) = left.commit_<(this)
  }
}

object FlowLayout {

  object FromLeft extends Facet(">>-", ">>- -->")
  object FromRight extends Facet("-<<", "<-- -<<")
}
