package com.tribbloids.spookystuff.web

import com.tribbloids.spookystuff.actions.Action

/**
  * Created by peng on 3/26/15.
  */
package object actions {

//  type GenTrace = Seq[Action]

  type Trace = List[Action]

  type DryRun = List[Trace]

}
