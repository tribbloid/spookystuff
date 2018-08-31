package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.{Doc, DocOption}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.{Const, QueryException, SpookyContext}
import org.apache.spark.ml.dsl.utils.Verbose
import org.apache.spark.ml.dsl.utils.messaging.AutomaticRelay
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.slf4j.LoggerFactory

object ActionLike extends AutomaticRelay[ActionLike] {

  //TODO: aggregate all object that has children
  case class TreeNodeView(
      actionLike: ActionLike
  ) extends TreeNode[TreeNodeView] {

    override def children: Seq[TreeNodeView] = actionLike.children.map {
      TreeNodeView
    }

    override def verboseString: String = simpleString
  }
}

@SerialVersionUID(8566489926281786854L)
abstract class ActionLike extends Product with Serializable with Verbose {

  override val productPrefix = this.getClass.getSimpleName.stripSuffix("$")

  def children: Trace

  lazy val TreeNode: ActionLike.TreeNodeView = ActionLike.TreeNodeView(this)

  def globalRewriters: Seq[RewriteRule[Trace]] = Nil

  /**
    * invoked on executors, immediately after interpolation
    * *IMPORTANT!* may be called several times, before or after GenPartitioner.
    **/
  def localRewriters: Seq[MonadicRewriteRule[Trace]] = Nil

  final def interpolate(row: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val result = this.doInterpolate(row, schema)
    result.foreach { action =>
      action.injectFrom(this)
    }
    result
  }

  //TODO: use reflection to simplify
  /**
    * convert all extractor constructor parameters to Literals
    */
  def doInterpolate(row: FetchedRow, schema: SpookySchema): Option[this.type] = Some(this)

  def injectFrom(same: ActionLike): Unit = {} //TODO: change to immutable pattern to avoid one Trace being used twice with different names

  //  final def injectTo(same: ActionLike): Unit = same.injectFrom(this)

  //used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def hasOutput: Boolean = outputNames.nonEmpty

  def outputNames: Set[String] = Set.empty

  /**
    * create a list of list each denoting the anticipated backtrace of each output.
    * @return
    */
  def dryrun: DryRun

  //the minimal equivalent action that can be put into backtrace
  def skeleton: Option[this.type] = Some(this)

  def apply(session: Session): Seq[DocOption]

  def fetch(spooky: SpookyContext): Seq[DocOption] = {

    val results = CommonUtils.retry(Const.remoteResourceLocalRetries) {
      fetchOnce(spooky)
    }
    val numPages = results.count(_.isInstanceOf[Doc])
    spooky.spookyMetrics.pagesFetched += numPages

    results
  }

  def fetchOnce(spooky: SpookyContext): Seq[DocOption] = {

    if (!this.hasOutput) return Nil

    val pagesFromCache: Seq[Seq[DocOption]] =
      if (!spooky.spookyConf.cacheRead) Seq(null)
      else
        dryrun.map(
          dry =>
            InMemoryDocCache
              .get(dry, spooky)
              .orElse {
                DFSDocCache.get(dry, spooky)
              }
              .orNull
        )

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
          case e: Throwable =>
            spooky.spookyMetrics.fetchFromRemoteFailure += 1
            throw e
        }
      }
    }
  }
}
