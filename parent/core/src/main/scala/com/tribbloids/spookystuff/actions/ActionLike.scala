package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Trace.DryRun
import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Agent
import com.tribbloids.spookystuff.tree.TreeView
import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.{Const, QueryException, SpookyContext}
import com.tribbloids.spookystuff.relay.AutomaticRelay
import org.apache.spark.ml.dsl.utils.{DurationJSONSerializer, Verbose}
import org.json4s.Formats
import org.slf4j.LoggerFactory

object ActionLike extends AutomaticRelay[ActionLike] {

  override lazy val fallbackFormats: Formats = super.fallbackFormats + DurationJSONSerializer

  // TODO: aggregate all object that has children
  case class TreeNodeView(
      actionLike: ActionLike
  ) extends TreeView.Immutable[TreeNodeView] {

    override def children: Seq[TreeNodeView] = actionLike.children.map {
      TreeNodeView
    }
  }
}

@SerialVersionUID(8566489926281786854L)
abstract class ActionLike extends Product with Serializable with Verbose {

//  override lazy val productPrefix: String = this.getClass.getSimpleName.stripSuffix("$")

  def children: Trace

  lazy val TreeNode: ActionLike.TreeNodeView = ActionLike.TreeNodeView(this)

  def globalRewriteRules(schema: SpookySchema): Seq[RewriteRule[Trace]] = Nil

  /**
    * invoked on executors, immediately after interpolation *IMPORTANT!* may be called several times, before or after
    * GenPartitioner.
    */
  def localRewriteRules(schema: SpookySchema): Seq[RewriteRule[Trace]] = Nil

  final def interpolate(row: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val result = this.doInterpolate(row, schema)
    result.foreach { action =>
      action.injectFrom(this)
    }
    result
  }

  // TODO: can be made automatic
  // TODO: this.type cleanup
  /**
    * convert all extractor constructor parameters to Literals
    */
  def doInterpolate(row: FetchedRow, schema: SpookySchema): Option[this.type] = Some(this)

  def injectFrom(same: ActionLike): Unit = {}
  // TODO: change to immutable pattern to avoid one Trace being used twice with different names

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
  // TODO: this.type cleanup
  def skeleton: Option[this.type] = Some(this)

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
      if (!spooky.spookyConf.cacheRead) Seq(null)
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

      if (!spooky.spookyConf.remote)
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
