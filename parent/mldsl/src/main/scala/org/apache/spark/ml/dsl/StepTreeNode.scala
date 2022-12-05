package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.tree.TreeView
import org.apache.spark.ml.dsl.utils.messaging.{MessageAPI, Relay}
import org.apache.spark.sql.utils.DataTypeRelay

trait StepTreeNode[BaseType <: StepTreeNode[BaseType]] extends TreeView.Immutable[StepTreeNode[BaseType]] {

  val self: StepLike

  lazy val paths: Seq[Seq[String]] = {
    val rootPath = Seq(self.name)
    if (children.nonEmpty) {
      children.flatMap { child =>
        child.paths.map(_ ++ rootPath)
      }
    } else Seq(rootPath)
  }

  lazy val mergedPath: Seq[String] = {

    val numPaths = paths.map(_.size)
    assert(numPaths.nonEmpty, "impossible")
    val result = {
      val maxBranchLength = numPaths.max
      val commonAncestorLength = maxBranchLength
        .to(0, -1)
        .find { v =>
          paths.map(_.slice(0, v)).distinct.size == 1
        }
        .getOrElse(0)
      val commonAncestor = paths.head.slice(0, commonAncestorLength)

      val commonParentLength = maxBranchLength
        .to(0, -1)
        .find { v =>
          paths.map(_.reverse.slice(0, v)).distinct.size == 1
        }
        .getOrElse(0)
      val commonParent = paths.head.reverse.slice(0, commonParentLength).reverse

      if (commonAncestor.size + commonParent.size > maxBranchLength) commonParent
      else commonAncestor ++ commonParent
    }
    result
  }
}

object StepTreeNode extends Relay.<<[StepTreeNode[_]] {

  override def toMessage_>>(v: StepTreeNode[_]): Msg = {
    val base = v.self match {
      case source: Source =>
        Msg(
          source.id,
          dataTypes = source.dataTypes
            .map(DataTypeRelay.toMessage_>>)
        )
      case _ =>
        Msg(v.self.id)
    }
    base.copy(
      stage = v.children.map(this.toMessage_>>)
    )
  }

  case class Msg(
      id: String,
      dataTypes: Set[DataTypeRelay.Msg] = Set.empty,
      stage: Seq[Msg] = Nil
  ) extends MessageAPI.<< {
    override def toProto_<< : StepTreeNode[_] = ???
  }
}
