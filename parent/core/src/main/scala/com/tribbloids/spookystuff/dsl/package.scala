package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.agent.WebProxySetting
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.row.LocalityGroup

// should import DSL directly, instead of package dsl.
package object dsl extends DSL {

  type ByDoc[R] = :=>[Doc, R]
  type ByTrace[R] = :=>[Trace, R]
  type WebProxyFactory = :=>[Unit, WebProxySetting]

  type GenPartitioner = GenPartitionerLike[LocalityGroup, LocalityGroup]
  type AnyGenPartitioner = GenPartitionerLike[LocalityGroup, Any]
}
