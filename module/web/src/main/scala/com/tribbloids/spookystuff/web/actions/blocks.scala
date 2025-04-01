//package com.tribbloids.spookystuff.web.actions
//
//import com.tribbloids.spookystuff.*
//import com.tribbloids.spookystuff.actions.{ControlBlock, DocCondition, Loop, Trace}
//import com.tribbloids.spookystuff.doc.{Doc, Observation}
//import com.tribbloids.spookystuff.row.{AgentRow, SpookySchema}
//import com.tribbloids.spookystuff.agent.Agent
//
//import scala.collection.mutable.ArrayBuffer
//import scala.concurrent.duration.Duration
//
////case class cannot be overridden
////syntax sugar for loop-click-wait
//object LoadMore {
//
//  def apply(
//             selector: String,
//             limit: Int = ControlBlock.maxLoop,
//             delay: Duration = Const.Interaction.delayMin
//  ): Loop =
//    Loop(
//      Click(selector, cooldown = delay),
//      limit
//    )
//}
//
//object Paginate {
//
//  def apply(
//             selector: String,
//             limit: Int = ControlBlock.maxLoop,
//             delay: Duration = Const.Interaction.delayMin
//  ): Loop = {
//    Loop(
//      Snapshot()
//        +> Click(selector, cooldown = delay),
//      limit
//    )
//  }
//}
//
//final case class WebDocIf(
//    condition: DocCondition, // TODO: merge with Extraction[Boolean]
//    ifTrue: Trace,
//    ifFalse: Trace
//) extends ControlBlock(ifTrue ++ ifFalse: Trace) {
//
//  override def skeleton: Option[WebDocIf.this.type] =
//    Some(this.copy(ifTrue = ifTrue.flatMap(_.skeleton), ifFalse = ifFalse.flatMap(_.skeleton)))
//
//  override def doExeNoUID(agent: Agent): Seq[Observation] = {
//
//    val current = Snapshot.QuickSnapshot.exe(agent).head.asInstanceOf[Doc]
//
//    val pages = new ArrayBuffer[Observation]()
//    if (condition(current -> agent)) {
//      for (action <- ifTrue) {
//        pages ++= action.exe(agent)
//      }
//    } else {
//      for (action <- ifFalse) {
//        pages ++= action.exe(agent)
//      }
//    }
//
//    pages.toSeq
//  }
//
//  override def _copy(children: Trace): ControlBlock = this.copy()
//}
