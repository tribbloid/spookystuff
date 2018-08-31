package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.Lambda

/**
  * Created by peng on 3/26/15.
  */
package object actions {

//  type GenTrace = Seq[Action]

  type Trace = List[Action]

  type DryRun = List[Trace]

  type DocFilter = Lambda[(Doc, Session), Doc] //TODO: merge with Selector[Doc]

  type DocCondition = Lambda[(Doc, Session), Boolean] //TODO: merge with Selector[Doc]
}
