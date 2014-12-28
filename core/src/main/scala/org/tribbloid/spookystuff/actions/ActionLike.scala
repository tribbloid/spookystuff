package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.session.Session
import org.tribbloid.spookystuff.pages.{PageLike, Page}

/**
 * Created by peng on 11/7/14.
 */
//can't extend function, will override toString() of case classes
trait ActionLike
  extends Serializable
  with Product {

  final def interpolate(pr: PageRow): Option[this.type] = {
    val result = this.doInterpolate(pr)
    result.foreach(_.inject(this))
    result
  }

  def doInterpolate(pageRow: PageRow): Option[this.type] = Some(this)

  def inject(same: this.type ): Unit = {} //do nothing

  //used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def mayExport: Boolean = outputNames.nonEmpty

  def outputNames: Set[String]

  //the minimal equivalent action that can be put into backtrace
  def trunk: Option[this.type]

  def apply(session: Session): Seq[PageLike]
}
