package com.tribbloids.spookystuff.utils

import org.apache.spark.ml.dsl.utils.FlowUtils
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object TreeException {

  case class TreeNodeView(self: Throwable) extends TreeNode[TreeNodeView] {
    override def children: Seq[TreeNodeView] = {
      val result = self match {
        case v: TreeException =>
          v.causes.map(TreeNodeView)
        case _ =>
          val eOpt = Option(self).flatMap(
            v =>
              Option(v.getCause)
          )
          eOpt.map(TreeNodeView).toSeq
      }
      result.sortBy(_.simpleString)
    }

    override def simpleString: String = {
      self match {
        case v: TreeException =>
          v.simpleMessage
        case _ =>
          self.getClass.getName + ": " + self.getMessage
      }
    }

    override def verboseString: String = simpleString + "\n" + FlowUtils.stackTracesShowStr(self.getStackTrace)
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
              agg: Seq[Throwable] => Throwable = {
                es =>
                  wrap(es)
              },
              extra: Seq[Throwable] = Nil
            ): Seq[T] = {

    val es = trials.collect {
      case Failure(e) => e
    }
    if (es.isEmpty) {
      trials.map(_.get)
    }
    else {
      throw agg(extra.flatMap(v => Option(v)) ++ es)
    }
  }

  def |||[T](
              trials: Seq[Try[T]],
              agg: Seq[Throwable] => Throwable = {
                es =>
                  wrap(es)
              },
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
    }
    else {
      results
    }
  }

  /**
    * early exit
    *
    * @param trials
    * @param agg
    * @param extra
    * @tparam T
    * @return
    */
  def |||^[T](
               trials: Seq[() => T],
               agg: Seq[Throwable] => Throwable = {
                 es =>
                   wrap(es)
               },
               extra: Seq[Throwable] = Nil
             ): Option[T] = {

    val buffer = ArrayBuffer[Try[T]]()

    if (trials.isEmpty) return None

    val results = for (fn <- trials) yield {

      val result = Try{fn()}
      result match {
        case Success(t) => return Some(t)
        case _ =>
      }
      result
    }

    if (results.nonEmpty) {
      val es = results.collect {
        case Failure(e) => e
      }
      throw agg(extra.flatMap(v => Option(v)) ++ es)
    }
    else {
      throw new UnknownError("IMPOSSIBLE!")
    }
  }

  class Node(
              val simpleMessage: String = "",
              val cause: Throwable = null
            ) extends TreeException {

    override def causes: Seq[Throwable] = {

      cause match {
        case Wrapper(causes) => causes
        case _ =>
          Option(cause).toSeq
      }
    }
  }

  /**
    *
    * @param es
    * @param upliftUnary not recommended to set to false, should use Wrapper() directly for type safety
    * @return
    */
  def wrap(es: Seq[Throwable], upliftUnary: Boolean = true): Throwable = {
    require(es.nonEmpty, "No exception")

    if (es.size == 1 && upliftUnary) {
      es.head
    }
    else {
      Wrapper(causes = es)
    }
  }

  case class Wrapper(
                      override val causes: Seq[Throwable] = Nil
                    ) extends TreeException {

    val simpleMessage: String = s"[CAUSED BY ${causes.size} EXCEPTION(S)]"
  }
}

trait TreeException extends Throwable {

  import com.tribbloids.spookystuff.utils.TreeException.TreeNodeView

  def causes: Seq[Throwable] = Nil

  lazy val treeNodeView = TreeNodeView(this)

  override def getMessage: String = treeNodeView.toString()

  override def getCause: Throwable = causes.headOption.orNull

  def simpleMessage: String
}
