
package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.{DriverSession, Session}
import org.apache.spark.ml.dsl.utils.{FlowUtils, MessageWrapper}

import scala.language.dynamics
import scala.util.Random

object PyAction {

  final val QQQ = "\"\"\""
}

trait PyAction extends Action {

  //TODO: how to clean up variable? Should I prefer a stateless/functional design?
  val varPrefix = FlowUtils.toCamelCase(this.getClass.getSimpleName)
  val varName = varPrefix + Math.abs(Random.nextLong())

  //can only be overriden by lazy val
  //Python class must have a constructor that takes json format of its Scala class
  def constructPython(session: Session): String = {
    // java & python have different namespace convention.
    val pyFullName = Seq("pyspookystuff") ++
      this.getClass.getCanonicalName
        .stripPrefix("com.tribbloids.spookystuff")
        .split('.')
        .filter(_.nonEmpty)
    val pyPackage = pyFullName.slice(0, pyFullName.length -1).mkString(".")
    val pyClass = pyFullName.last
    val lines = Seq(
      s"from $pyPackage import $pyClass",
      s"$varName = $pyClass (",
      PyAction.QQQ,
      this.toJSON,
      PyAction.QQQ,
      ")"
    )
    val result = session.pythonDriver.interpret(lines.mkString("\n"))
    varName
  }

  def destructPython(session: Session): Unit = {
    session.pythonDriver.interpret(
      s"del $varName"
    )
    Unit
  }

  case class Py(session: Session) extends Dynamic {

    def applyDynamic(methodName: String)(args: Any*): Seq[String] = {
      val argJSONs: Seq[String] = args.map {
        v =>
          val json = MessageWrapper(v).toJSON()
          Seq(
            PyAction.QQQ,
            json,
            PyAction.QQQ
          )
            .mkString("\n")
      }
      val lines = Seq(
        s"$varName.$methodName (",
        argJSONs.mkString(",\n"),
        ")"
      )
      val result = session.pythonDriver.interpret(lines.mkString("\n"))

      result
    }
  }

  /**
    * must have exactly the same class/function under the same package imported in python that takes 2 JSON strings
    * 1 for action, 1 for session
    */
  override def exe(session: Session): Seq[Fetched] = {

    session match {
      case d: DriverSession =>
        d.initializePythonDriverIfMissing()
      case _ =>
    }
    constructPython(session)

    try {
      super.exe(session)
    }
    finally {
      destructPython(session)
    }
  }
}