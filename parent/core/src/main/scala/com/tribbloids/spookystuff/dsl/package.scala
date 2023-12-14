package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.compose.Fn1
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.WebProxySetting

// should import DSL directly, instead of package dsl.
package object dsl extends DSL {

  type ByDoc[+R] = Fn1[Doc, R]
  type ByTrace[+R] = Fn1[Trace, R]
  type WebProxyFactory = Fn1[Unit, WebProxySetting]

  type GenPartitioner = GenPartitionerLike[TraceView, TraceView]
  type AnyGenPartitioner = GenPartitionerLike[TraceView, Any]
}
