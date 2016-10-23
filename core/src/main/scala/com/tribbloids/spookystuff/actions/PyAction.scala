
package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.{DriverSession, Session}
import org.apache.spark.ml.dsl.utils.{FlowUtils, Message, MessageView}
import org.json4s.JsonAST.{JArray, JObject}

import scala.language.dynamics
import scala.util.Random

object PyAction {

  final val QQQ = "\"\"\""
}

trait PyArgsMarshaller {

  def obj2Args(v: Any): String
}

object PyArgsMarshaller {

  object JSON extends PyArgsMarshaller {

    import org.json4s.jackson.JsonMethods._

    def obj2Args(v: Any) = {
      message2Args(MessageView(v))
    }

    def message2Args(message: Message): String = {

      val jv = message.toJValue()
      val unpackStar = jv match {
        case j: JObject => "**"
        case j: JArray => "*"
        case _ => new UnsupportedOperationException("Marshaller only supports array, map or object")
      }
      val json = pretty(render(jv))
      val code =
        s"""
           |$unpackStar(json.loads(
           |${PyAction.QQQ}
           |$json
           |${PyAction.QQQ}
           |))
        """.stripMargin
      code
    }
  }
}

trait PyAction extends Action {

  def marshaller = PyArgsMarshaller.JSON

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
    val code =
      s"""
         |from $pyPackage import $pyClass
         |
        |$varName = $pyClass(
         |${marshaller.message2Args(this.toMessage)}
         |)
      """.stripMargin

    session.pythonDriver.interpret(code)
    varName
  }

  def destructPython(session: Session): Unit = {
    session.pythonDriver.interpret(
      s"del $varName"
    )
    Unit
  }

  case class Py(session: Session) extends Dynamic {

    def applyDynamic(methodName: String)(args: Any*): String = {

      val code =
        s"""
           |result = $varName.$methodName(
           |${marshaller.obj2Args(args)}
           |)
        """.stripMargin
      val result = session.pythonDriver.execute(code)._2

      result.getOrElse("Nil")
    }

    def applyDynamicNamed(methodName: String)(args: (String, Any)*): String = {

      val code =
        s"""
           |result = $varName.$methodName(
           |${marshaller.obj2Args(Map(args: _*))}
           |)
        """.stripMargin
      val result = session.pythonDriver.execute(code)._2

      result.getOrElse("Nil")
    }
  }

  /**
    * must have exactly the same class/function under the same package imported in python that takes 2 JSON strings
    * 1 for action, 1 for session
    */
  override def exe(session: Session): Seq[Fetched] = {

    session match {
      case d: DriverSession =>
        d.getOrCreatePythonDriver
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