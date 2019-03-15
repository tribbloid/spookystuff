package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.DSL.Facet
import com.tribbloids.spookystuff.graph.Module.Heads

trait FlowDSL[I <: Impl] extends DSL[I] {

  object FromLeft extends Facet("->>")
  object FromRight extends Facet("<<-")

  lazy val facets = List(FromLeft, FromRight)

  trait Interface extends super.Interface {

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

    def union(another: Interface): Self = {
      core.union(another.core)
    }
    def U(another: Interface) = union(another)

    // left > right

    def merge_>(right: Interface): Self = `:>>`.merge(right.core)
    def merge(right: Interface): Self = merge_>(right)
    def >>>(right: Interface): Self = merge_>(right)
    def >(right: Interface): Self = merge_>(right)

    //TODO: fast-forward handling: if right is reused for many times, ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
    def rebase_>(right: Interface): Self = `:>>`.rebase(right.core)
    def rebase(right: Interface): Self = rebase_>(right)
    def >=>(right: Interface): Self = rebase_>(right)

    //this is really kind of ambiguous
    def commit_>(right: Interface): Self = `:>>`.commit(right.core)
    def commit(right: Interface): Self = commit_>(right)
    def >->(right: Interface): Self = commit_>(right)

    // left < right
    //TODO: follow :>> & <<: convention

    def merge_<(left: Interface): Self = `<<:`.merge(left.core)
    def egrem(prev: Interface) = prev.merge_<(this)
    def <<<(prev: Interface) = prev.merge_<(this)
    def <(prev: Interface) = prev.merge_<(this)

    def rebase_<(left: Interface): Self = `<<:`.rebase(left.core)
    def esaber(prev: Interface) = prev.rebase_<(this)
    def <=<(prev: Interface) = prev.rebase_<(this)

    def commit_<(left: Interface): Self = `<<:`.commit(left.core)
    def timmoc(left: Interface) = left.commit_<(this)
    def <-<(left: Interface) = left.commit_<(this)
  }
}
