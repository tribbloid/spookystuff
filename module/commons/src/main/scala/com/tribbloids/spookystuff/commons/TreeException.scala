package com.tribbloids.spookystuff.commons

import ai.acyclic.prover.commons.typesetting.TextBlock
import ai.acyclic.prover.commons.util.Causes

import scala.util.{Failure, Success, Try}

object TreeException {

  case class _TreeView(self: Throwable) extends TreeView {
    override def children: Seq[_TreeView] = {
      val result: Seq[_TreeView] = self match {
        case v: Causes.HasCauses[?] =>
          v.causes.map(_TreeView.apply)
        case _ =>
          val eOpt = Option(self).flatMap(v => Option(v.getCause))
          eOpt.map(_TreeView.apply).toSeq
      }
      result.sortBy(_.nodeText)
    }

    override def nodeText: String = {
      self match {
        case v: TreeException =>
          val msgBlock = TextBlock(v.getMessage_simple)
          msgBlock.build

//          if (msgBlock.lines.size > 1) { TODO: remove, only useful if subgraph is required
//            msgBlock.pad.left(Padding.leftSquare).build
//          } else {
//            msgBlock.build
//          }
        case _ =>
          self.getClass.getName + ": " + self.getMessage
      }
    }

  }

  object AllMustSucceed {

    def apply[T](
        attempts: Seq[Try[T]],
        combine: Seq[Throwable] => Throwable = Causes.combine(_),
        extra: Seq[Throwable] = Nil
    ): Seq[T] = {

      val es = attempts.collect {
        case Failure(e) => e
      }
      if (es.isEmpty) {
        attempts.map(_.get)
      } else {
        val ee = combine(extra.flatMap(v => Option(v)) ++ es)
        throw ee
      }
    }
  }

  def &&& = AllMustSucceed

  object OneMustSucced {

    def apply[T](
        attempts: Seq[Try[T]],
        agg: Seq[Throwable] => Throwable = Causes.combine(_),
        extra: Seq[Throwable] = Nil
    ): Seq[T] = {

      if (attempts.isEmpty) return Nil

      val results = attempts.collect {
        case Success(e) => e
      }

      if (results.isEmpty) {
        val es = attempts.collect {
          case Failure(e) => e
        }
        throw agg(extra.flatMap(v => Option(v)) ++ es)
      } else {
        results
      }
    }
  }

  def ||| = OneMustSucced

  object GetFirstSuccess {

    /**
      * early exit
      */
    def apply[T](
        attempts: Seq[() => T],
        agg: Seq[Throwable] => Throwable = Causes.combine(_),
        extra: Seq[Throwable] = Nil
    ): Option[T] = {

      if (attempts.isEmpty) return None

      val errors = for (fn <- attempts) yield {

        val result = Try(fn())
        result match {
          case Success(t) => return Some(t)
          case Failure(e) => e
        }
      }

      if (errors.nonEmpty) {
        throw agg(extra.flatMap(v => Option(v)) ++ errors)
      } else {
        throw new UnknownError("IMPOSSIBLE!")
      }
    }
  }

  def |||^ = GetFirstSuccess

}

trait TreeException extends Causes.HasCauses[Throwable] {

  import com.tribbloids.spookystuff.commons.TreeException._TreeView

  override def getCause: Throwable = null

  override def causes: Seq[Throwable] = {
    val cause = getCause
    cause match {
      case v: Causes.HasCauses[?] =>
        v.causes
      case _ =>
        Option(cause).toSeq
    }
  }
  lazy val treeView: _TreeView = _TreeView(this)

  override def getMessage: String = "\n" + treeView.treeString()

  def getMessage_simple: String
}
