package com.tribbloids.spookystuff.row

object __RowDesign {

  /**
    * This package requires exactly 1 "backbone" Row implementation, the RDD of which will serve as the underlying data
    * structure of ExecutionPlan.
    *
    * Such implementation should satisfy the following requirements:
    *
    *   - should be serializable
    *   - should be an atomic w.r.t. [[LocalityGroup]], redistributing rows should not destroy locality
    *   - trajectories (series of agent observations) can be generated,cached & serialized (default behaviour, this
    *     makes serialized form fairly large, but it is OK in persisting/checkpointing - local/distributed FS space is
    *     cheap), but can be discarded manually if the agent has to replay trajectory from scratch (e.g. ExecutionPlan
    *     asks to perform more agent actions, but RDD is already shuffled, and old agent is no longer reachable)
    *     - even if no more agent action is required, you may still consider discarding trajectories just to make
    *       serialized form smaller - network IO is still expensive!
    *   - under the above premise, shrink serialized form as much as possible
    *
    * Assuming the following Conventions:
    *
    *   - [[AgentState]] contains the the agent (initialized or inherited on demand, a.k.a
    *     [[com.tribbloids.spookystuff.session.Session]]), its backtrace (agent play history), and its latest trajectory
    *   - [[DataRow]] is a lightweight data structure that can be mapped to a Spark SQL row
    *   - [[DataRow.WithScope]] is a lightweight structure that contains a [[DataRow]] and pointers to parts of
    *     trajectory (namesake of "Scope")
    *   - [[SquashedRow]] contains an [[AgentState]] and several [[DataRow.WithScope]]s
    *   - [[Delta]] is a function: RowState => RowState
    *   - [[LocalityGroup]] contains a deterministic trace of agent actions shared by all [[Delta]]s within a backbone
    *     row, as its name suggested, used to make fetch/explore on cluster more efficient
    *
    * There are 2 competing designs for such row:
    *
    *   - \1. SquashedRow: consisting of a [[LocalityGroup]], unmodified [[DataRow]]s, and a series of [[Delta]], each
    *     play the agent or modify [[DataRow]]s in a certain way. Upon rollout, all [[Delta]]s being executed will
    *     attach a trajectory, which can be discarded later on demand
    *     - this has some weaknesses:
    *     - RDD is already a placeholder of delta, which has much better support. To shuffle/persist a bottleneck RDD,
    *       it is far more easier to use an RDD from further upstream.
    *     - Several RDDs consisting of SquashedRows that are identical in data but differs only in delta may be created.
    *       How would you decide which one to persist/checkpoint? Should they all be persisted?
    *
    *   - \2. SquashedRow: consisting of a [[LocalityGroup]], modified [[DataRow.WithScope]]s, and an [[AgentState]].
    *     When [[AgentState]] is serialized, the trajectory can be discarded on demand, but the agent is always
    *     discarded, recreated on deserialization, and replay all the backtrace to reach the same state
    *     - this has some weaknesses:
    *     - serialization of AgentState needs to be implemented, and it is not easy!
    *     - ScopedRow's pointers may become dangling after an agent replay, this problem doesn't exist in (1), because
    *       scope pointers are generated by [[Delta]]s. But in (2), it could happen rarely, when the RDD is persisted
    *       and later used with agent replay
    *     - due to the weakness of (1), this is likely the chosen design
    *
    * but here is the interesting part, What if most Trajectory have small serialized form? Their binary content will be
    * auto-saved into DFS somewhere, and can be discarded in serialization.
    *
    * this also solved a problem in [[com.tribbloids.spookystuff.caching.DFSDocCache]]: once this feature is merged with
    * auto-save, any content will be saved into DFS files, while only the other part of the trajectory (very small,
    * mostly metadata) will be serialized and saved into other DFS files, this makes RDD shuffling faster, while
    * [[com.tribbloids.spookystuff.caching.DFSDocCache]] more space efficient.
    * [[com.tribbloids.spookystuff.caching.InMemoryDocCache]] won't be affected, it doesn't even use serialization.
    *
    * If we can pull this off, there will be no need to discard trajectory, agent replay will only be used for new
    * contents. For (2), dangling pointers will be impossible
    */
}
