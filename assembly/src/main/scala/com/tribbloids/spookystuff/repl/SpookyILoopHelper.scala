package com.tribbloids.spookystuff.repl

import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SQLContext

import scala.collection.immutable
import scala.tools.nsc.interpreter.{Results, NamedParam}

trait SpookyILoopHelper extends SparkILoop { //TODO: merge ispooky initialization into this to avoid recompling

  private val spookyScript =
    "import scala.concurrent.duration._" ::
      "import com.tribbloids.spookystuff.actions._" ::
      "import com.tribbloids.spookystuff.dsl._" ::
      "import com.tribbloids.spookystuff.SpookyContext" ::
      "val spooky = new SpookyContext(sql)" ::
      "import spooky.dsl._" ::
      Nil

  def loadSpooky(): Unit = {
    this.beQuietDuring {
      val sql = new SQLContext(this.sparkContext)
      val sqlParam = NamedParam[SQLContext]("sql", sql)
      this.bind(sqlParam.name, sqlParam.tpe, sqlParam.value, immutable.List("@transient")) match {
        case Results.Success =>
        case _ => throw new RuntimeException("SQL failed to initialize")
      }

      spookyScript.foreach(s => assert(this.interpret(s) == Results.Success))
    }
  }

  override protected def postInitialization() = {

    super.postInitialization()
    loadSpooky()
  }

  override def prompt: String = "spooky> "
}
