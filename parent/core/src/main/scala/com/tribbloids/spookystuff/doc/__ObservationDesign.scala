package com.tribbloids.spookystuff.doc

object __ObservationDesign {

  /**
    * the main implementation of [[Observation]] is [[Doc]]. Its name is imprecise for many agents (e.g. agents that
    * play video games), and may be corrected later when RL becomes a main concern
    *
    * [[Doc]] represents the following 2 parts of information:
    *   - Content: observed data in raw byte array, usually large in size and can be opened as a file. As result, in
    *     most cases it should be saved as a file on DFS, and be excluded from serialization & shipping, the shipped
    *     object can easily reconstruct its Content by reading from the same file.
    *   - Others (including [[com.tribbloids.spookystuff.actions.Trace]],
    *     [[com.tribbloids.spookystuff.io.ResourceMetadata]] etc.): describes the observation process, usually small in
    *     size and can be easily serialized
    *
    * Content can be Raw or Parsed (by Apache Tika), [[com.tribbloids.spookystuff.agent.Agent]] is only capable of
    * observing Raw content, if it has an unsupported format, Apache Tika will be used to convert it into Parsed. Both
    * can be optionally converted into Saved by [[com.tribbloids.spookystuff.caching.DFSDocCache]], which has a minimal
    * but slower serialized form, as the actual content is saved as a file on DFS that can be opened for inspection.
    */
}
