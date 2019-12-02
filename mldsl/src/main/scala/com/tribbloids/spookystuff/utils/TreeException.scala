package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.TreeException._combine
import org.apache.spark.ml.dsl.utils.FlowUtils
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.util.{Failure, Success, Try}

object TreeException {

  case class TreeNodeView(self: Throwable) extends TreeNode[TreeNodeView] {
    override def children: Seq[TreeNodeView] = {
      val result = self match {
        case v: TreeException =>
          v.causes.map(TreeNodeView)
        case _ =>
          val eOpt = Option(self).flatMap(
            v => Option(v.getCause)
          )
          eOpt.map(TreeNodeView).toSeq
      }
      result.sortBy(_.simpleString)
    }

    override def simpleString: String = {
      self match {
        case v: TreeException =>
          v.simpleMsg
        case _ =>
          self.getClass.getName + ": " + self.getMessage
      }
    }

    override def verboseString: String =
      simpleString + "\n" +
        FlowUtils.stackTracesShowStr(self.getStackTrace)
  }

  sealed trait MonadicUndefined extends Throwable

  object Undefined extends MonadicUndefined

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
      agg: Seq[Throwable] => Throwable = combine(_),
      extra: Seq[Throwable] = Nil
  ): Seq[T] = {

    val es = trials.collect {
      case Failure(e) => e
    }
    if (es.isEmpty) {
      trials.map(_.get)
    } else {
      throw agg(extra.flatMap(v => Option(v)) ++ es)
    }
  }

  def |||[T](
      trials: Seq[Try[T]],
      agg: Seq[Throwable] => Throwable = combine(_),
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
      agg: Seq[Throwable] => Throwable = combine(_),
      extra: Seq[Throwable] = Nil
  ): Option[T] = {

//    val buffer = ArrayBuffer[Try[T]]()

    if (trials.isEmpty) return None

    val results = for (fn <- trials) yield {

      val result = Try { fn() }
      result match {
        case Success(t) => return Some(t)
        case _          =>
      }
      result
    }

    if (results.nonEmpty) {
      val es = results.collect {
        case Failure(e) => e
      }
      throw agg(extra.flatMap(v => Option(v)) ++ es)
    } else {
      throw new UnknownError("IMPOSSIBLE!")
    }
  }

  /**
    * @param upliftUnary not recommended to set to false, should use Wrapper() directly for type safety
    * @return
    */
  def combine(causes: Seq[Throwable], upliftUnary: Boolean = true): Throwable = {
    val _causes = causes.distinct
    if (_causes.isEmpty) Undefined

    if (_causes.size == 1 && upliftUnary) {
      _causes.head
    } else {
      _combine(causes = _causes)
    }
  }

  def monadicCombine(causes: Seq[Throwable], upliftUnary: Boolean = true): Throwable = {
    val undefined = causes.find(_.isInstanceOf[MonadicUndefined])
    undefined.getOrElse(
      combine(causes, upliftUnary)
    )
  }

  protected case class _combine(
      override val causes: Seq[Throwable] = Nil
  ) extends TreeException {

    val simpleMsg: String = s"[CAUSED BY ${causes.size} EXCEPTION(S)]"
  }
}

trait TreeException extends Throwable {

  import com.tribbloids.spookystuff.utils.TreeException.TreeNodeView

  override def getCause: Throwable

  def causes: Seq[Throwable] = {
    val cause = getCause
    cause match {
      case _combine(causes) => causes
      case _ =>
        Option(cause).toSeq
    }
  }
  lazy val treeNodeView = TreeNodeView(this)

  override def getMessage: String = treeNodeView.treeString(verbose = false)

  def simpleMsg: String
}
