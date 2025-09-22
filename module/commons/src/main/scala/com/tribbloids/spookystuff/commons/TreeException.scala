package com.tribbloids.spookystuff.commons

import ai.acyclic.prover.commons.typesetting.TextBlock
import ai.acyclic.prover.commons.util.Causes
import ai.acyclic.prover.commons.util.Causes.Undefined

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

  def &&&[T](
      trials: Seq[Try[T]],
      combine: Seq[Throwable] => Throwable = Causes.combine(_),
      extra: Seq[Throwable] = Nil
  ): Seq[T] = {

    val es = trials.collect {
      case Failure(e) => e
    }
    if (es.isEmpty) {
      trials.map(_.get)
    } else {
      val ee = combine(extra.flatMap(v => Option(v)) ++ es)
      throw ee
    }
  }

  def |||[T](
      trials: Seq[Try[T]],
      agg: Seq[Throwable] => Throwable = Causes.combine(_),
      extra: Seq[Throwable] = Nil
  ): Seq[T] = {

    if (trials.isEmpty) return Nil

    val results = trials.collect {
      case Success(e) => e
    }

    if (results.isEmpty) {
      val es = trials.collect {
        case Failure(e) => e
      }
      throw agg(extra.flatMap(v => Option(v)) ++ es)
    } else {
      results
    }
  }

  /**
    * early exit
    */
  def |||^[T](
      trials: Seq[() => T],
      agg: Seq[Throwable] => Throwable = Causes.combine(_),
      extra: Seq[Throwable] = Nil
  ): Option[T] = {

    if (trials.isEmpty) return None

    val errors = for (fn <- trials) yield {

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

  /**
    * same as [[combine]], except that any [[Undefined]] detected will cause the output to be also [[Undefined]]
    * indicating that a lack of trials is the ultimate cause and can be situationally ignored
    * @param causes
    *   all direct causes of this throwable
    * @param foldUnary
    *   not recommended to set to false, should use Wrapper() directly for type safety
    * @return
    */
  def parallel(causes: Seq[Throwable], foldUnary: Boolean = true): Throwable = {
    val undefined = causes.find(_.isInstanceOf[Undefined])
    undefined.getOrElse(
      Causes.combine(causes, foldUnary)
    )
  }

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
