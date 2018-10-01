package com.tribbloids.spookystuff.utils

class CommonConst {

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
  val clusterRetries = 3

  object Interaction {

    val delayMax: Duration = 60.seconds
    val delayMin: Duration = 0.second
    val blocking: Boolean = true
  }

  val hardTerminateOverhead: Duration = 20.seconds

  val USER_DIR = System.getProperty("user.dir")
  val TEMP_DIR = CommonUtils.\\\(USER_DIR, "temp")
  val ROOT_TEMP_DIR = System.getProperty("java.io.tmpdir")
  val UNPACK_RESOURCE_DIR = CommonUtils.\\\(ROOT_TEMP_DIR, "spookystuff", "resources")
}

object CommonConst extends CommonConst
