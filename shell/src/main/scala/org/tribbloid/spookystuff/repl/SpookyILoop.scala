package org.tribbloid.spookystuff.repl

import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SQLContext
import org.tribbloid.spookystuff.SpookyContext

import scala.collection.immutable
import scala.tools.nsc.interpreter._

class SpookyILoop extends SparkILoop {

  private val postInitScript =
    "import scala.concurrent.duration._" ::
      "import org.tribbloid.spookystuff.actions._" ::
      "import org.tribbloid.spookystuff.factory.driver._" ::
      "import org.tribbloid.spookystuff.expressions._" ::
      "import org.tribbloid.spookystuff.SpookyContext" ::
      "import sql._" ::
      "import spooky._" ::
      Nil

  override protected def postInitialization() {
    super.postInitialization()
    this.beQuietDuring {
      val sql = new SQLContext(this.sparkContext)
      val sqlParam = NamedParam[SQLContext]("sql", sql)
      intp.bind(sqlParam.name, sqlParam.tpe, sqlParam.value, immutable.List("@transient")) match {
        case IR.Success =>
        case _ => throw new RuntimeException("SQL failed to initialize")
      }

      val spooky = new SpookyContext(sql)
      val spookyParam = NamedParam[SpookyContext]("spooky", spooky)
      intp.bind(spookyParam.name, spookyParam.tpe, spookyParam.value, immutable.List("@transient")) match {
        case IR.Success =>
        case _ => throw new RuntimeException("SpookyContext failed to initialize")
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
