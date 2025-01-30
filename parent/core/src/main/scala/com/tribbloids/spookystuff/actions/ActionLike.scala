package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Trace.DryRun
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.commons.{CommonUtils, TreeView, Verbose}
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import com.tribbloids.spookystuff.relay.AutomaticRelay
import com.tribbloids.spookystuff.relay.io.DurationJSONSerializer
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.{Const, QueryException, SpookyContext}
import org.json4s.Formats
import org.slf4j.LoggerFactory

object ActionLike extends AutomaticRelay[ActionLike] {

  override lazy val formats: Formats = super.formats + DurationJSONSerializer

  // TODO: aggregate all object that has children
  case class TreeNodeView(
      actionLike: ActionLike
  ) extends TreeView.Immutable[TreeNodeView] {

    override def children: Seq[TreeNodeView] = actionLike.children.map {
      TreeNodeView.apply
    }
  }
}

@SerialVersionUID(8566489926281786854L)
abstract class ActionLike extends Product with Serializable with Verbose {

  def children: Trace

  lazy val TreeNode: ActionLike.TreeNodeView = ActionLike.TreeNodeView(this)

  /**
    * invoked on executors, immediately after definition *IMPORTANT!* may be called several times, before or after
    * GenPartitioner.
    */
  def localRewriteRules[D](schema: SpookySchema): Seq[RewriteRule[Trace]] = Nil

  def injectFrom(same: ActionLike): Unit = {}
  // TODO: remove, or change to immutable pattern to avoid one Trace being used twice with different names

  //  final def injectTo(same: ActionLike): Unit = same.injectFrom(this)

  // used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def hasOutput: Boolean = outputNames.nonEmpty

  def outputNames: Set[String] = Set.empty

  /**
    * create a list of list each denoting the anticipated backtrace of each output.
    * @return
    */
  def dryRun: DryRun

  // the minimal equivalent action that can be put into backtrace
  def skeleton: Option[ActionLike] = Some(this)

  def apply(agent: Agent): Seq[Observation]

  def fetch(spooky: SpookyContext): Seq[Observation] = {

    val results = CommonUtils.retry(Const.remoteResourceLocalRetries) {
      fetchOnce(spooky)
    }
    val numPages = results.count(_.isInstanceOf[Doc])
    spooky.spookyMetrics.pagesFetched += numPages

    results
  }

  def fetchOnce(spooky: SpookyContext): Seq[Observation] = {

    if (!this.hasOutput) return Nil

    val pagesFromCache: Seq[Seq[Observation]] =
      if (!spooky.conf.cacheRead) Seq(null)
      else
        dryRun.map { dry =>
          val view = Trace(dry)
          InMemoryDocCache
            .get(view, spooky)
            .orElse {
              DFSDocCache.get(view, spooky)
            }
            .orNull
        }

    if (!pagesFromCache.contains(null)) {
      spooky.spookyMetrics.fetchFromCacheSuccess += 1

      val results = pagesFromCache.flatten
      spooky.spookyMetrics.pagesFetchedFromCache += results.count(_.isInstanceOf[Doc])
      this.children.foreach { action =>
        LoggerFactory.getLogger(this.getClass).info(s"(cached)+> ${action.toString}")
      }

      results
    } else {
      spooky.spookyMetrics.fetchFromCacheFailure += 1

      if (!spooky.conf.remote)
        throw new QueryException(
          "Resource is not cached and not allowed to be fetched remotely, " +
            "the later can be enabled by setting SpookyContext.conf.remote=true"
        )
      spooky.withSession { session =>
        try {
          val result = this.apply(session)
          spooky.spookyMetrics.fetchFromRemoteSuccess += 1
          result
        } catch {
          case e: Exception =>
            spooky.spookyMetrics.fetchFromRemoteFailure += 1
            session.Drivers.releaseAll()
            throw e
        }
      }
    }
  }
}
