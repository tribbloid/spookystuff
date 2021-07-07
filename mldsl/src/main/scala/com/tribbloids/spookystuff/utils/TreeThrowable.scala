package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.TreeThrowable.ExceptionWithCauses
import org.apache.spark.ml.dsl.utils.DSLUtils
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.util.{Failure, Success, Try}

object TreeThrowable {

  case class TreeNodeView(self: Throwable) extends TreeNode[TreeNodeView] {
    override def children: Seq[TreeNodeView] = {
      val result = self match {
        case v: TreeThrowable =>
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
        case v: TreeThrowable =>
          v.simpleMsg
        case _ =>
          self.getClass.getName + ": " + self.getMessage
      }
    }

    override def verboseString: String =
      simpleString + "\n" +
        DSLUtils.stackTracesShowStr(self.getStackTrace)
  }

  /**
    * Not a real throwable, just a placeholder indicating lack of trials
    */
  sealed trait Undefined extends Throwable
  object Undefined extends Undefined

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
      val ee = agg(extra.flatMap(v => Option(v)) ++ es)
      throw ee
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
    val _causes = causes.distinct.filterNot(_.isInstanceOf[Undefined])
    if (_causes.isEmpty) Undefined

    if (_causes.size == 1 && upliftUnary) {
      _causes.head
    } else {
      ExceptionWithCauses(causes = _causes)
    }
  }

  /**
    * same as [[combine]], except that any [[Undefined]] detected will cause the output to be also [[Undefined]]
    * indicating that a lack of trials is the ultimate cause and can be situationally ignored
    * @param causes
    * @param upliftUnary not recommended to set to false, should use Wrapper() directly for type safety
    * @return
    */
  def monadicCombine(causes: Seq[Throwable], upliftUnary: Boolean = true): Throwable = {
    val undefined = causes.find(_.isInstanceOf[Undefined])
    undefined.getOrElse(
      combine(causes, upliftUnary)
    )
  }

  protected case class ExceptionWithCauses(
      override val causes: Seq[Throwable] = Nil
  ) extends Exception
      with TreeThrowable {

    override def getCause: Throwable = causes.headOption.orNull

    val simpleMsg: String = s"[CAUSED BY ${causes.size} EXCEPTION(S)]"
  }
}

trait TreeThrowable extends Throwable {

  import com.tribbloids.spookystuff.utils.TreeThrowable.TreeNodeView

  def causes: Seq[Throwable] = {
    val cause = getCause
    cause match {
      case ExceptionWithCauses(causes) => causes
      case _                           => Option(cause).toSeq
    }
  }
  lazy val treeNodeView: TreeNodeView = TreeNodeView(this)

  override def getMessage: String = treeNodeView.treeString(verbose = false)

  def simpleMsg: String
}
