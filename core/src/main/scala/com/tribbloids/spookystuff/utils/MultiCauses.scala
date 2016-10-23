package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.MultiCauses.TreeNodeView
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.util.{Failure, Try}

object MultiCauses {

  case class TreeNodeView(self: Throwable) extends TreeNode[TreeNodeView] {
    override def children: Seq[TreeNodeView] = {
      self match {
        case v: MultiCauses =>
          v.causes.map(TreeNodeView)
        case _ =>
          val eOpt = Option(self).flatMap(
            v =>
              Option(v.getCause)
          )
          eOpt.map(TreeNodeView).toSeq
      }
    }

    override def simpleString(): String = {
      self match {
        case v: MultiCauses =>
          v.simpleMessage
        case _ =>
          self.getClass.getName + ": " + self.getMessage
      }
    }
  }

  def &&&[T](trials: Seq[Try[T]], agg: Seq[Throwable] => MultiCauses = WithCauses): Seq[T] = {
    val es = trials.collect{
      case Failure(e) => e
    }
    if (es.isEmpty) {
      trials.map(_.get)
    }
    else {
      throw agg(es)
    }
  }
}

trait MultiCauses extends Throwable {

  def causes: Seq[Throwable] = Nil

  lazy val treeNodeView = TreeNodeView(this)

  override def getMessage: String = treeNodeView.toString()

  def simpleMessage: String = "[MULTIPLE CAUSES]"
}

case class WithCauses(override val causes: Seq[Throwable] = Nil) extends MultiCauses