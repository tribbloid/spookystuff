package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.caching.{DFSWebCache, InMemoryWebCache}
import com.tribbloids.spookystuff.doc.{Doc, Fetched}
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.{DetailedProduct, SpookyUtils}
import com.tribbloids.spookystuff.{Const, QueryException, SpookyContext}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.slf4j.LoggerFactory

object ActionLike {

  case class TreeNodeView(
                           actionLike: ActionLike
                         ) extends TreeNode[TreeNodeView] {

    override def children: Seq[TreeNodeView] = actionLike.children.map {
      TreeNodeView
    }
  }
}

@SerialVersionUID(8566489926281786854L)
abstract class ActionLike extends DetailedProduct with Serializable {

  def children: Trace

  lazy val TreeNode: ActionLike.TreeNodeView = ActionLike.TreeNodeView(this)

  //TODO: this step should be broken into 2 stages for better efficiency, f1 =(resolve on driver)=> f2 =(eval on executors)=> v
  final def interpolate(pr: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val option = this.doInterpolate(pr, schema)
    option.foreach{
      action =>
        action.injectFrom(this)
    }
    option
  }

  //TODO: use reflection to simplify
  /**
    * convert all extractor constructor parameters to Literal
    */
  def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = Some(this)

  def injectFrom(same: ActionLike): Unit = {} //TODO: change to immutable pattern to avoid one Trace being used twice with different names

//  final def injectTo(same: ActionLike): Unit = same.injectFrom(this)

  //used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def hasOutput: Boolean = outputNames.nonEmpty

  def outputNames: Set[String]

  def dryrun: DryRun

  //the minimal equivalent action that can be put into backtrace
  def trunk: Option[this.type]

  def apply(session: Session): Seq[Fetched]

  def fetch(spooky: SpookyContext): Seq[Fetched] = {

    val results = SpookyUtils.retry (Const.remoteResourceLocalRetries){
      fetchOnce(spooky)
    }
    val numPages = results.count(_.isInstanceOf[Doc])
    spooky.metrics.pagesFetched += numPages

    results
  }

  def fetchOnce(spooky: SpookyContext): Seq[Fetched] = {

    if (!this.hasOutput) return Nil

    val pagesFromCache: Seq[Seq[Fetched]] = if (!spooky.conf.cacheRead) Seq(null)
    else dryrun.map (
      dry =>
        InMemoryWebCache.get(dry, spooky)
          .orElse{
            DFSWebCache.get(dry, spooky)
          }
          .orNull
    )

    if (!pagesFromCache.contains(null)){
      spooky.metrics.fetchFromCacheSuccess += 1

      val results = pagesFromCache.flatten
      spooky.metrics.pagesFetchedFromCache += results.count(_.isInstanceOf[Doc])
      this.children.foreach{
        action =>
          LoggerFactory.getLogger(this.getClass).info(s"(cached)+> ${action.toString}")
      }

      results
    }
    else {
      spooky.metrics.fetchFromCacheFailure += 1

      if (!spooky.conf.remote) throw new QueryException(
        "Resource is not cached and not allowed to be fetched remotely, " +
          "the later can be enabled by setting SpookyContext.conf.remote=true"
      )

      val session = new Session(spooky)

      try {
        val result = this.apply(session)
        spooky.metrics.fetchFromRemoteSuccess += 1
        result
      }
      catch {
        case e: Throwable =>
          spooky.metrics.fetchFromRemoteFailure += 1
          throw e
      }
      finally {
        session.tryClean()
      }
    }
  }
}

//object ActionLikeRelay extends SimpleStructRelay[ActionLike] {
//
//  //this class assumes that all extractions has been cast into literals
//  case class Repr(
//                   className: String,
//                   uri: Option[String],
//                   selector: Option[Selector], //TODO: switch to Doc => Unstructured
//                   delay: Option[Duration],
//                   blocking: Option[Boolean],
//                   maxDelay: Option[Duration],
//                   text: Option[String],
//                   value: Option[String],
//                   script: Option[String],
//                   handleSelector: Option[Selector],
//                   DocFilter: Option[DocFilter],
//                   self: Option[Repr],
//                   children: Option[Seq[Repr]],
//                   retries: Option[Int],
//                   limit: Option[Int],
//                   cacheError: Option[Boolean],
//                   cacheEmptyOutput: Option[Boolean],
//                   condition: Option[DocCondition],
//                   ifTrue: Option[Seq[Repr]],
//                   ifFalse: Option[Seq[Repr]]
//                 ) extends StructRepr[ActionLike] {
//
//    override def toSelf: ActionLike = ???
//  }
//}