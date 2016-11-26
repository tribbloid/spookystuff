package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.TreeException.TreeNodeView
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.util.{Failure, Try}

object TreeException {

  case class TreeNodeView(self: Throwable) extends TreeNode[TreeNodeView] {
    override def children: Seq[TreeNodeView] = {
      self match {
        case v: TreeException =>
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
        case v: TreeException =>
          v.nodeMessage
        case _ =>
          self.getClass.getName + ": " + self.getMessage
      }
    }
  }

  def &&&[T](trials: Seq[Try[T]], agg: Seq[Throwable] => TreeException = es => new Node(causes = es)): Seq[T] = {
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

  class Unary(
               val nodeMessage: String = "",
               val cause: Throwable = null
             ) extends TreeException {

    override def causes: Seq[Throwable] = Option(cause).toSeq
  }

  class Node(
              val nodeMessage: String = "[CAUSED BY MULTIPLE EXCEPTIONS]",
              override val causes: Seq[Throwable] = Nil
            ) extends TreeException {

  }
}

trait TreeException extends Throwable {

  def causes: Seq[Throwable] = Nil

  lazy val treeNodeView = TreeNodeView(this)

  override def getMessage: String = treeNodeView.toString()

  override def getCause: Throwable = causes.headOption.orNull

  def nodeMessage: String
}
