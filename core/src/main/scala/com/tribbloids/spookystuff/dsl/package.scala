package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.WebProxySetting
import com.tribbloids.spookystuff.utils.{Lambda0, Lambda}

import scala.language.implicitConversions

// should import DSL directly, instead of package dsl.
package object dsl extends DSL {

  type ByDoc[+R] = Lambda[Doc, R]
  type ByTrace[+R] = Lambda[Trace, R]
  type WebProxyFactory = Lambda0[WebProxySetting]

  type GenPartitioner = GenPartitionerLike[TraceView, TraceView]
  type AnyGenPartitioner = GenPartitionerLike[TraceView, Any]
}
