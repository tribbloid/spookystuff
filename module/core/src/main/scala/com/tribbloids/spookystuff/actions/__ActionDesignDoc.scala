package com.tribbloids.spookystuff.actions

object __ActionDesignDoc {

  /**
    * CAUTION:
    *
    *   - when returning multiple traces at the same time, try returning TraceSet instead, it has built-in deduplication
    *     and can easily avoid returning empty result
    *
    * this API will gradually move to define-by-run/tracing model:
    *
    *   - all control blocks will be delegated to Scala language
    *   - agent state will be exposed directly in every function input that accepts AgentRow
    *   - in planning stage, this function will be executed against a fake, dry-run/tracing only, agent
    *   - ... the result is a trace representing the **best-effort** speculated dry-run/partially evaluated docUID from
    *     known data
    *   - locality grouping will use this trace to ensure that agent assigned similar tasks are deployed as closely as
    *     possible
    *   - this process is deterministic & axiomatic, can it be make empirical?
    *   - sometimes, even with unknown data it is possible to speculate trace or multiple alternative traces, can these
    *     situation be handled (also a problem in kernel fusion in SIMD machine learning)?
    */
}
