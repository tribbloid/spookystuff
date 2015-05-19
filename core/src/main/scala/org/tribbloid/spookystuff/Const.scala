package org.tribbloid.spookystuff

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFiles
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.dsl.LeftOuter

/**
 * Created by peng on 04/06/14.
 */
object Const {

  import scala.concurrent.duration._

  val maxLoop = Int.MaxValue

  val defaultCharset = "ISO-8859-1"

  val defaultJoinType = LeftOuter

  //  val webClientOptions = new WebClientOptions
  //  webClientOptions.setUseInsecureSSL(true)

  val sessionInitializationTimeout = 40.seconds

  val localResourceLocalRetry = 3
  val remoteResourceLocalRetry = 2
  val DFSLocalRetry = 2

  val interactionDelayMax: Duration = 60.seconds
  val interactionDelayMin: Duration = 2.seconds
  val interactionBlock: Boolean = true

  val hardTerminateOverhead: Duration = 20.seconds

  def phantomJSPath = Option(System.getenv("PHANTOMJS_PATH"))
    .getOrElse{
    LoggerFactory.getLogger(this.getClass).info("$PHANTOMJS_PATH does not exist, using tempfile instead")
    SparkFiles.get(phantomJSFileName)
  }

  //used in sc.addFile(...)
  val phantomJSUrl = System.getenv("PHANTOMJS_PATH") //TODO: download it from public resource
  private val phantomJSFileName = new Path(phantomJSUrl).getName

  val defaultInputKey = "_"
  val keyDelimiter = "'"
  val onlyPageWildcard = "$"
  val allPagesWildcard = "$_*"
  val defaultJoinKey = "A"

  val jsonFormats = DefaultFormats
}