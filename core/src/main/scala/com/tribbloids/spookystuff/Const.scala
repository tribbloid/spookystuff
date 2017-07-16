package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.row.Field
import org.apache.tika.detect.DefaultDetector
import org.json4s.DefaultFormats
import com.tribbloids.spookystuff.dsl.DocFilters

object Const {

  import scala.concurrent.duration._

  val maxLoop = Int.MaxValue

  val defaultTextCharset = "ISO-8859-1"
  val defaultApplicationCharset = "UTF-8"

  //  val webClientOptions = new WebClientOptions
  //  webClientOptions.setUseInsecureSSL(true)

  //TODO: move to SpookyConf as much as possible
  val sessionInitializationTimeout = 40.seconds

  val localResourceLocalRetries = 3 //In-node/partition retries
  val remoteResourceLocalRetries = 2 //In-node/partition retries
  val DFSLocalRetries = 2
  val DFSBlockedAccessRetries = 10
  val clusterRetries = 3

  object Interaction {

    val delayMax: Duration = 60.seconds
    val delayMin: Duration = 0.seconds
    val blocking: Boolean = true
  }

  val hardTerminateOverhead: Duration = 20.seconds

  val defaultInputKey = "_"
  val keyDelimiter = "'"
  val onlyPageExtractor = "S"
  val allPagesExtractor = "S_*"

  val groupIndexExtractor = "G"

  val defaultJoinField = Field("A", isWeak = true)

  val mimeDetector = new DefaultDetector()

  val defaultDocumentFilter = DocFilters.MustHaveTitle
  val defaultImageFilter = DocFilters.AcceptStatusCode2XX

  val exploreStageSize = 100
}