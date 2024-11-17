package com.tribbloids.spookystuff.commons

import ai.acyclic.prover.commons.util.Causes
import ai.acyclic.prover.commons.util.Causes.Undefined

import scala.util.{Failure, Success, Try}

object TreeThrowable {

  // TODO: rename to _TreeVew, keep consistency
  case class TreeNodeView(self: Throwable) extends TreeView.Immutable[TreeNodeView] {
    override def children: Seq[TreeNodeView] = {
      val result = self match {
        case v: TreeThrowable =>
          v.causes.map(TreeNodeView.apply)
        case _ =>
          val eOpt = Option(self).flatMap(v => Option(v.getCause))
          eOpt.map(TreeNodeView.apply).toSeq
      }
      result.sortBy(_.simpleString(0))
    }

    override def simpleString(maxFields: Int): String = {
      self match {
        case v: TreeThrowable =>
          v.simpleMsg
        case _ =>
          self.getClass.getName + ": " + self.getMessage
      }
    }
  }

  //  def aggregate(
  //                 fn: Seq[Throwable] => Throwable,
  //                 extra: Seq[Throwable] = Nil
  //               )(seq: Seq[Throwable]): Throwable = {
  //
  //    val flat = seq.flatMap {
  //      case Wrapper(causes) =>
  //        causes
  //      case v@_ => Seq(v)
  //    }
  //    val all = extra.flatMap(v => Option(v)) ++ flat
  //    fn(all)
  //  }

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

trait TreeThrowable extends Throwable {

  import com.tribbloids.spookystuff.commons.TreeThrowable.TreeNodeView

  def causes: Seq[Throwable] = {
    val cause = getCause
    cause match {
      case Causes(causes) => causes
      case _              => Option(cause).toSeq
    }
  }
  lazy val treeNodeView: TreeNodeView = TreeNodeView(this)

  override def getMessage: String = treeNodeView.treeString(verbose = false)

  def simpleMsg: String
}
