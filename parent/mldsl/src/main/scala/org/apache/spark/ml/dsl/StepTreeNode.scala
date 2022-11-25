package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.tree.TreeView
import org.apache.spark.ml.dsl.utils.messaging.{MessageAPI_<<, MessageRelay}
import org.apache.spark.sql.utils.DataTypeRelay

trait StepTreeNode[BaseType <: StepTreeNode[BaseType]] extends TreeView.Immutable[StepTreeNode[BaseType]] {

  val self: StepLike

  override protected def stringArgs: Iterator[String] = mergedPath.iterator

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

object StepTreeNode extends MessageRelay[StepTreeNode[_]] {

  override def toMessage_>>(v: StepTreeNode[_]): M = {
    val base = v.self match {
      case source: Source =>
        M(
          source.id,
          dataTypes = source.dataTypes
            .map(DataTypeRelay.toMessage_>>)
        )
      case _ =>
        M(v.self.id)
    }
    base.copy(
      stage = v.children.map(this.toMessage_>>)
    )
  }

  case class M(
      id: String,
      dataTypes: Set[DataTypeRelay.M] = Set.empty,
      stage: Seq[M] = Nil
  ) extends MessageAPI_<< {
    override def toProto_<< : StepTreeNode[_] = ???
  }
}
