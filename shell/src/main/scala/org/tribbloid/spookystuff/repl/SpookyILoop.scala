package org.tribbloid.spookystuff.repl

import org.apache.spark.repl.SparkILoop

class SpookyILoop extends SparkILoop {

  private val postInitScript =
    "import org.tribbloid.spookystuff.SpookyContext._" :: //TODO: this is obsolete, revise according to ISpark counterpart
      "import org.tribbloid.spookystuff.entity._" ::
      Nil

  override protected def postInitialization() {
    super.postInitialization()
    this.beQuietDuring {
      postInitScript.foreach(command)
    }
  }

  override def prompt: String = "spooky> "

  override def printWelcome(): Unit = {
    echo(
      """
        |      ____                / /
        |     / __/__  ___  ___   / /__ _   _
        |    _\ \/ _ \/ _ \/ _ \ /  '_// |_/ /
        |   /___/ .__/\___/\___//_/\_\ \_.  /  STUFF  version 0.2.0
        |      / /                      _/ /
        |     / /                      /__/

      """)
    import scala.tools.nsc.Properties._
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in queries to have them executed.")
    echo("Type :help for more information.")
  }
}
