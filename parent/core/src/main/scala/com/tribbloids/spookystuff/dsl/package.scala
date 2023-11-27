package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.PreDef.Fn
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.row.LocalityGroup
import com.tribbloids.spookystuff.session.WebProxySetting

// should import DSL directly, instead of package dsl.
package object dsl extends DSL {

  type ByDoc[R] = Fn[Doc, R]
  type ByTrace[R] = Fn[Trace, R]
  type WebProxyFactory = Fn[Unit, WebProxySetting]

  type GenPartitioner = GenPartitionerLike[LocalityGroup, LocalityGroup]
  type AnyGenPartitioner = GenPartitionerLike[LocalityGroup, Any]
}
