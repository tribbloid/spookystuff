package com.tribbloids.spookystuff.utils

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
      result.sortBy(_.simpleString())
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

  def aggregate(
                 fn: Seq[Throwable] => Throwable,
                 extra: Seq[Throwable] = Nil,
                 expandUnary: Boolean = false
               ): Seq[Throwable] => Throwable = {

    {
      seq =>
        val flat = seq.flatMap {
          case MultiCauseWrapper(causes) =>
            causes
          case v@_ => Seq(v)
        }
        val all = extra.flatMap(v => Option(v)) ++ flat
        if (expandUnary && all.size == 1) all.head
        else fn(all)
    }
  }

  def &&&[T](
              trials: Seq[Try[T]],
              agg: Seq[Throwable] => Throwable = {
                es =>
                  if (es.size == 1) {
                    es.head
                  }
                  else {
                    MultiCauseWrapper(causes = es)
                  }
              },
              extra: Seq[Throwable] = Nil,
              expandUnary: Boolean = false
            ): Seq[T] = {

    val es = trials.collect {
      case Failure(e) => e
    }
    if (es.isEmpty) {
      trials.map(_.get)
    }
    else {
      val _agg = aggregate(agg, extra, expandUnary)
      throw _agg(es)
    }
  }

  def |||[T](
              trials: Seq[Try[T]],
              agg: Seq[Throwable] => Throwable = {
                es =>
                  if (es.size == 1) {
                    es.head
                  }
                  else {
                    MultiCauseWrapper(causes = es)
                  }
              },
              extra: Seq[Throwable] = Nil,
              expandUnary: Boolean = false
            ): Seq[T] = {

    if (trials.isEmpty) return Nil

    val results = trials.collect {
      case Success(e) => e
    }

    if (results.isEmpty) {
      val es = trials.collect {
        case Failure(e) => e
      }
      val _agg = aggregate(agg, extra, expandUnary)
      throw _agg(es)
    }
    else {
      results
    }
  }

  /**
    * early exit
    * @param trials
    * @param agg
    * @param extra
    * @param expandUnary
    * @tparam T
    * @return
    */
  def |||^[T](
               trials: Seq[() => T],
               agg: Seq[Throwable] => Throwable = {
                 es =>
                   if (es.size == 1) {
                     es.head
                   }
                   else {
                     MultiCauseWrapper(causes = es)
                   }
               },
               extra: Seq[Throwable] = Nil,
               expandUnary: Boolean = false
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
      val _agg = aggregate(agg, extra, expandUnary)
      throw _agg(es)
    }
    else {
      throw new RuntimeException("IMPOSSIBLE!")
    }
  }

  class Node(
              val nodeMessage: String = "",
              val cause: Throwable = null
            ) extends TreeException {

    override def causes: Seq[Throwable] = {
      cause match {
        case MultiCauseWrapper(causes) => causes
        case _ =>
          Option(cause).toSeq
      }
    }
  }

  case class MultiCauseWrapper(
                                override val causes: Seq[Throwable] = Nil
                              ) extends TreeException {

    val nodeMessage: String = s"[CAUSED BY ${causes.size} EXCEPTION(S)]"
  }
}

trait TreeException extends Throwable {

  import com.tribbloids.spookystuff.utils.TreeException.TreeNodeView

  def causes: Seq[Throwable] = Nil

  lazy val treeNodeView = TreeNodeView(this)

  override def getMessage: String = treeNodeView.toString()

  override def getCause: Throwable = causes.headOption.orNull

  def nodeMessage: String
}
