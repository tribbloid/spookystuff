package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.Impl
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.agent.WebProxySetting
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.row.LocalityGroup

// should import DSL directly, instead of package dsl.
package object dsl extends DSL {

  type ByDoc[R] = Impl.Fn[Doc, R]
  type ByTrace[R] = Impl.Fn[Trace, R]
  type WebProxyFactory = Impl.Fn[Unit, WebProxySetting]

  type GenPartitioner = GenPartitionerLike[LocalityGroup, LocalityGroup]
  type AnyGenPartitioner = GenPartitionerLike[LocalityGroup, Any]
}
