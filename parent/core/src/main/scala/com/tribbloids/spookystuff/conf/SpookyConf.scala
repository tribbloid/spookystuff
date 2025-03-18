package com.tribbloids.spookystuff.conf

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import com.tribbloids.spookystuff.agent.*
import com.tribbloids.spookystuff.commons.Timeout
import com.tribbloids.spookystuff.dsl.*
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import java.util.Date
import scala.concurrent.duration.*
import scala.concurrent.duration.Duration.Infinite

object SpookyConf {

  // DO NOT change to val! all confs are
  // mutable
  def default = new SpookyConf()

  val defaultHTTPHeaders: Map[String, String] = Map(
    "User-Agent" ->
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36"
  )
}

/**
  * Created by peng on 12/06/14. will be shipped to workers
  */
case class SpookyConf(
                       shareMetrics: Boolean = false, // TODO: not necessary

                       webProxy: WebProxyFactory = WebProxyFactory.NoProxy,
                       httpHeadersFactory: Unit :=> Map[String, String] = :=>.at(_ => SpookyConf.defaultHTTPHeaders),
                       oAuthKeysFactory: Unit :=> OAuthKeys = :=>.at(_ => null),
                       //    var browserResolution: (Int, Int) = (1920, 1080),
                       remote: Boolean = true, // if disabled won't use remote client at all
                       //
                       auditing: Auditing = Auditing.Both,
                       auditingFilePaths: DocPath = DocPath.UUIDName(TracePath.Hierarchical),
                       //
                       cacheWrite: Boolean = true,
                       cacheRead: Boolean = true, // TODO: this enable both in-memory and DFS cache, should allow more refined control

                       cachedDocsLifeSpan: Duration = 7.day,
                       IgnoreCachedDocsBefore: Option[Date] = None,
                       cacheFilePaths: TracePath = TracePath.Hierarchical,
                       //
                       errorDump: Boolean = true,
                       errorScreenshot: Boolean = true,
                       errorDumpFilePaths: DocPath = DocPath.UUIDName(TracePath.Hierarchical),
                       //
                       remoteResourceTimeout: Timeout = Timeout(60.seconds),
                       DFSTimeout: Timeout = Timeout(40.seconds),
                       failOnDFSRead: Boolean = false,
                       //
                       locality: Locality = Locality.Wide(),
                       //
                       selectSampling: Sampler = Sampler.Identity,
                       fetchSampling: Sampler = Sampler.Identity, // takes remote actions and cost much more than flatten.
                       exploreSampling: Sampler = Sampler.Identity, // takes remote actions and cost much more than flatten.
                       //
                       explorePathPlanning: PathPlanning = PathPlanning_Simple.BreadthFirst,
                       exploreRange: Range = 0 until Int.MaxValue,
                       exploreEpochInterval: Int = 50,
                       exploreCheckpointInterval: Int = 50, // disabled if <=0

                       // if encounter too many out of memory error, change to MEMORY_AND_DISK_SER
                       defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
) extends Core.ConfLike
    with Serializable {

  override def importFrom(sparkConf: SparkConf): SpookyConf = {
    this
  }

  def getEarliestDocCreationTime(nowMillis: Long = System.currentTimeMillis()): Long = {

    val earliestTimeFromDuration = cachedDocsLifeSpan match {
      case _: Infinite => Long.MinValue
      case d =>
        nowMillis - d.toMillis
    }
    IgnoreCachedDocsBefore match {
      case Some(expire) =>
        Math.max(expire.getTime, earliestTimeFromDuration)
      case None =>
        earliestTimeFromDuration
    }
  }

  def previewMode: SpookyConf = {

    val sampler: Sampler.FirstN = Sampler.FirstN(1)

    this.copy(
      selectSampling = sampler,
      fetchSampling = sampler,
      exploreRange = 0 to 2
    )
  }
}
