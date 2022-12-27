package com.tribbloids.spookystuff.utils

class CommonConst {

  import scala.concurrent.duration._

  val maxLoop: Int = Int.MaxValue

  val defaultTextCharset: String = "ISO-8859-1"
  val defaultApplicationCharset: String = "UTF-8"

  val driverClosingTimeout: FiniteDuration = 5.seconds
  val driverClosingRetries: Int = 5

  val localResourceLocalRetries: Int = 3 // In-node/partition retries
  val remoteResourceLocalRetries: Int = 2 // In-node/partition retries
  val DFSLocalRetries: Int = 2
  val clusterRetries: Int = 3

  object Interaction {

    val delayMax: Duration = 60.seconds
    val delayMin: Duration = 0.second
    val blocking: Boolean = true
  }

  val hardTerminateOverhead: Duration = 20.seconds

  val USER_HOME: String = System.getProperty("user.home")
  val USER_DIR: String = System.getProperty("user.dir")

  val TEMP: String = "temp"

  val USER_TEMP_DIR: String = CommonUtils.\\\(USER_DIR, TEMP)
  val ROOT_TEMP_DIR: String = System.getProperty("java.io.tmpdir")

  val HADOOP_TEMP_DIR: String = "/tmp"

  val UNPACK_RESOURCE_DIR: String = CommonUtils.\\\(ROOT_TEMP_DIR, "spookystuff", "resources")
}

object CommonConst extends CommonConst
