package org.tribbloid.spookystuff.repl

import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SQLContext

import scala.collection.immutable
import scala.tools.nsc.interpreter.{Results, NamedParam}

class SpookyILoop extends SparkILoop {

  private val postInitScript =
    "import scala.concurrent.duration._" ::
      "import org.tribbloid.spookystuff.actions._" ::
      "import org.tribbloid.spookystuff.dsl._" ::
      "import org.tribbloid.spookystuff.expressions._" ::
      "import org.tribbloid.spookystuff.SpookyContext" ::
      "val spooky = new SpookyContext(sql)" ::
      "import spooky._" ::
      Nil

  override protected def postInitialization() {
    super.postInitialization()
    this.beQuietDuring {
      val sql = new SQLContext(this.sparkContext)
      val sqlParam = NamedParam[SQLContext]("sql", sql)
      intp.bind(sqlParam.name, sqlParam.tpe, sqlParam.value, immutable.List("@transient")) match {
        case Results.Success =>
        case _ => throw new RuntimeException("SQL failed to initialize")
      }

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
        |   /___/ .__/\___/\___//_/\_\ \_.  /  STUFF  version 0.3.0-SNAPSHOT
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
