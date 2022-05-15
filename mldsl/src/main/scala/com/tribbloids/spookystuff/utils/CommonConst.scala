package com.tribbloids.spookystuff.utils

class CommonConst {

  import scala.concurrent.duration._

  val maxLoop: Int = Int.MaxValue

  val defaultTextCharset = "ISO-8859-1"
  val defaultApplicationCharset = "UTF-8"

  val driverTerminationTimeout: FiniteDuration = 5.seconds

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

  val USER_DIR: String = System.getProperty("user.dir")

  val TEMP = "temp"

  val USER_TEMP_DIR: String = CommonUtils.\\\(USER_DIR, TEMP)
  val ROOT_TEMP_DIR: String = System.getProperty("java.io.tmpdir")

  val HADOOP_TEMP_DIR = "/tmp"

  val UNPACK_RESOURCE_DIR: String = CommonUtils.\\\(ROOT_TEMP_DIR, "spookystuff", "resources")
}

object CommonConst extends CommonConst
