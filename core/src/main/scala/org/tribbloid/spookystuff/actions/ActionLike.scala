package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.pages.PageLike
import org.tribbloid.spookystuff.session.Session

/**
 * Created by peng on 11/7/14.
 */
//can't extend function, will override toString() of case classes
trait ActionLike
  extends Serializable
  with Product {

  final def interpolate(pr: PageRow): Option[this.type] = {
    val results = this.doInterpolate(pr)
    results.foreach(_.injectFrom(this))
    results
  }

  def doInterpolate(pageRow: PageRow): Option[this.type] = Some(this)

  def injectFrom(same: this.type ): Unit = {} //do nothing

  //used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def hasExport: Boolean = outputNames.nonEmpty

  def outputNames: Set[String]

  //the minimal equivalent action that can be put into backtrace
  def trunk: Option[this.type]

  def apply(session: Session): Seq[PageLike]
}