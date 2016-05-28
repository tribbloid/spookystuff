package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.Session

/**
 * Created by peng on 3/26/15.
 */
package object actions {

  type Trace = List[Action]

  type DryRun = List[Trace]

  type DocFilter = ((Doc, Session) => Doc) //TODO: merge with Selector[Doc]

  type DocCondition = ((Doc, Session) => Boolean) //TODO: merge with Selector[Doc]

  type Selector = String //TODO: change to Doc => Element or Extraction
}
