package com.tribbloids.spookystuff.actions

import com.mchange.v2.c3p0.util.TestUtils
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.row.PageRow
import com.tribbloids.spookystuff.pages.Fetched
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.Utils

/**
 * Created by peng on 11/7/14.
 */
trait ActionLike extends Serializable {

  final def interpolate(pr: PageRow, context: SpookyContext): Option[this.type] = {
    val option = this.doInterpolate(pr, context)
    option.foreach{
      action =>
        action.injectFrom(this)
    }
    option
  }

  def doInterpolate(pageRow: PageRow, context: SpookyContext): Option[this.type] = Some(this)

  def injectFrom(same: ActionLike): Unit = {} //TODO: change to immutable pattern to avoid one Trace being used twice with different names

  final def injectTo(same: ActionLike): Unit = same.injectFrom(this)

  //used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def hasOutput: Boolean = outputNames.nonEmpty

  def outputNames: Set[String]

  //the minimal equivalent action that can be put into backtrace
  def trunk: Option[this.type]

  def apply(session: Session): Seq[Fetched]
}