package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.compose.Fn1
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Session

/**
  * Created by peng on 3/26/15.
  */
package object actions {

  type DocFilter = Fn1[(Doc, Session), Doc] // TODO: merge with Selector[Doc]

  type DocCondition = Fn1[(Doc, Session), Boolean] // TODO: merge with Selector[Doc]
}
