package com.tribbloids.spookystuff.execution

object __DefineByRun {

  /**
    * This will be the largest rewrite since its creation
    *
    * Once finished, the project will have the following capabilities:
    *
    * Stage 0:
    *
    *   - [[com.tribbloids.spookystuff.actions.Action]].interpolate will be gone, all their constructors only accept
    *     argument(s) with simple type
    *   - [[com.tribbloids.spookystuff.dsl.DataView]].fetch now takes a function, which can be either applied to a row
    *     with a real agent (in action), or applied to a row with a tracer (fake agent in dry-run-planning), the tracer
    *     can only record agent actions & return data placeholder (which are also tracer themselves), but return no real
    *     data
    *
    * Stage 1:
    *
    *   - define-by-run API defined in [[com.tribbloids.spookystuff.dsl.DataView]].apply that supersedes both fetch &
    *     extract
    *   - extract will be gone, fetch now becomes an alias of apply
    *   - define-by-run API takes a function [[com.tribbloids.spookystuff.row.AgentState]] =>
    *     [[com.tribbloids.spookystuff.row.Lineage]] as its only argument
    *   - [[com.tribbloids.spookystuff.row.AgentState]] can read from state of
    *     [[com.tribbloids.spookystuff.agent.DriverLike]]s directly, there is no need for Snapshot/Screenshot
    *
    * Stage 2:
    *
    *   - (optional) [[ExecutionPlan]] API can be merged into [[com.tribbloids.spookystuff.dsl.DataView]]
    *   - native soft/multi-pass execution mode: [[ExecutionPlan]]s can read from a Dataset, which represents result of
    *     a previous execution that partially succeeded: Each [[com.tribbloids.spookystuff.agent.Agent]] & extraction
    *     may fail for any reason (e.g. service unavailable, API or robot.txt quota exceeded, reached designated *
    *     sampling ratio).
    *     - As a result, [[ExecutionPlan]].run() should yields another [[ExecutionPlan]] of the same type, but with
    *       higher success rate
    *   - Self-healing capability: for each [[ExecutionPlan]] it is always desirable to fail immediately in its stage.
    *     - CAUTION: but this is not always possible, some error may be marked successful in a partially executed
    *       dataset, only to be discovered in later stage. A typical example of this case is to use a LLM inference
    *       service for dynamic web scraping:
    *       - the input collection of websites are sub-sampled and uploaded to a foundation LLM
    *       - the LLM is prompted to generate executable code/cssSelector/xpathSelector for interaction & data
    *         extraction, which are validated saved/checkpointed for repeated execution
    *       - the code is executed for every input website
    *       - if the generated code becomes faulty for any reason (e.g. hallucination, API changes, website layout
    *         changes), submit the faulty cases to step 1, start over for self-healing
    *       - if long chain of website interaction is required, this process should be repeated for several times.
    *         Current design can't automatically do this, how to improve or simplify?
    *       - Things will become more complex for e.g. robotic tasks described in Chain-of-Code
    */

}
