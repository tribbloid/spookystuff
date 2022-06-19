package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions.{Actions, Block, Loop}
import com.tribbloids.spookystuff.doc.{Doc, DocOption}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

//case class cannot be overridden
//syntax sugar for loop-click-wait
object LoadMore {

  def apply(
      selector: String,
      limit: Int = Const.maxLoop,
      delay: Duration = Const.Interaction.delayMin
  ): Loop =
    Loop(
      Click(selector, cooldown = delay),
      limit
    )
}

object Paginate {

  def apply(
      selector: String,
      limit: Int = Const.maxLoop,
      delay: Duration = Const.Interaction.delayMin
  ): Loop = {
    Loop(
      Snapshot()
        +> Click(selector, cooldown = delay),
      limit
    )
  }
}

final case class WebDocIf(
    condition: DocCondition, //TODO: merge with Extraction[Boolean]
    ifTrue: Trace,
    ifFalse: Trace
) extends Block(ifTrue ++ ifFalse) {

  override def skeleton: Option[WebDocIf.this.type] =
    Some(this.copy(ifTrue = ifTrue.flatMap(_.skeleton), ifFalse = ifFalse.flatMap(_.skeleton)).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[DocOption] = {

    val current = Snapshot.QuickSnapshot.exe(session).head.asInstanceOf[Doc]

    val pages = new ArrayBuffer[DocOption]()
    if (condition(current -> session)) {
      for (action <- ifTrue) {
        pages ++= action.exe(session)
      }
    } else {
      for (action <- ifFalse) {
        pages ++= action.exe(session)
      }
    }

    pages.toSeq
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val ifTrueInterpolated = Actions.doInterpolateSeq(ifTrue, pageRow, schema)
    val ifFalseInterpolated = Actions.doInterpolateSeq(ifFalse, pageRow, schema)
    val result = this.copy(ifTrue = ifTrueInterpolated, ifFalse = ifFalseInterpolated).asInstanceOf[this.type]
    Some(result)
  }
}
