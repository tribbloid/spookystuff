package org.tribbloid.spookystuff

/**
 * Created by peng on 3/26/15.
 */
package object actions {

  type Trace = Seq[Action]

  type DryRun = Seq[Trace]
}
