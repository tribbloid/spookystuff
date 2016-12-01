package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.AbstractSession

import scala.language.implicitConversions

/**
 * Created by peng on 3/26/15.
 */
package object actions {

  type Trace = List[Action]

  type DryRun = List[Trace]

  type DocFilter = ((Doc, AbstractSession) => Doc) //TODO: merge with Selector[Doc]

  type DocCondition = ((Doc, AbstractSession) => Boolean) //TODO: merge with Selector[Doc]

  type Selector = String //TODO: change to Doc => Element or Extraction
}