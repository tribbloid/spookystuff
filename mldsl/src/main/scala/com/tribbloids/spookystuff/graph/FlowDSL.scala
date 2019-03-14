package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.DSL.Facet
import com.tribbloids.spookystuff.graph.Module.Heads

trait FlowDSL[I <: Impl] extends DSL[I] {

  object FromLeft extends Facet("->>")
  object FromRight extends Facet("<<-")

  lazy val facets = List(FromLeft, FromRight)

  trait Interface[Self <: Interface[Self]] extends super.Interface[Self] {

    lazy val `:>>` = core.Ops(FromLeft, FromRight)
    lazy val `<<:` = core.Ops(FromRight, FromLeft)

    // select from

    def from(filter: EdgeFilter[DD]): Self = {
      val newFrom = Heads[DD](filter(core._graph))
      core.copy(
        fromOverride = Some(newFrom)
      )
    }

    //TODO: these symbols are lame
    def >-(filter: EdgeFilter[DD]): Self = from(filter)

    def and(filter: EdgeFilter[DD]) = {
      val newFrom = Heads[DD](filter(core._graph))
      val mergedFrom = Heads[DD](core.from.seq ++ newFrom.seq)
      core.copy(
        fromOverride = Some(mergedFrom)
      )
    }
    def <>-(filter: EdgeFilter[DD]): Self = and(filter)

    // undirected

    def union(another: Interface[Self]): Self = {
      core.union(another.core)
    }
    def U(another: Interface[Self]) = union(another)

    // left > right

    def merge_>(right: Interface[Self]): Self = core.`:>>`.merge(right.core)
    def merge(right: Interface[Self]): Self = merge_>(right)
    def >>>(right: Interface[Self]): Self = merge_>(right)
    def >(right: Interface[Self]): Self = merge_>(right)

    //TODO: fast-forward handling: if right is reused for many times, ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
    def rebase_>(right: Interface[Self]): Self = core.`:>>`.rebase(right.core)
    def rebase(right: Interface[Self]): Self = rebase_>(right)
    def >=>(right: Interface[Self]): Self = rebase_>(right)

    //this is really kind of ambiguous
    def commit_>(right: Interface[Self]): Self = core.`:>>`.commit(right.core)
    def commit(right: Interface[Self]): Self = commit_>(right)
    def >->(right: Interface[Self]): Self = commit_>(right)

    // left < right
    //TODO: follow :>> & <<: convention

    def merge_<(left: Interface[Self]): Self = core.`<<:`.merge(left.core)
    def egrem(prev: Interface[Self]): Self = prev.merge_<(this)
    def <<<(prev: Interface[Self]): Self = prev.merge_<(this)
    def <(prev: Interface[Self]): Self = prev.merge_<(this)

    def rebase_<(left: Interface[Self]): Self = core.`<<:`.rebase(left.core)
    def esaber(prev: Interface[Self]): Self = prev.rebase_<(this)
    def <=<(prev: Interface[Self]): Self = prev.rebase_<(this)

    def commit_<(left: Interface[Self]): Self = core.`<<:`.commit(left.core)
    def timmoc(left: Interface[Self]): Self = left.commit_<(this)
    def <-<(left: Interface[Self]): Self = left.commit_<(this)
  }
}
