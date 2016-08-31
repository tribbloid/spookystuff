
package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.{Session, SessionRelay}
import org.apache.spark.ml.dsl.utils.{FlowUtils, MessageWrapper}
import org.openqa.selenium.interactions.{Actions => SeleniumActions}

import scala.language.dynamics
import scala.util.Random

trait PyAction extends Action {

  //TODO: how to clean up variable? Should I prefer a stateless/functional design?
  val varPrefix = FlowUtils.toCamelCase(this.getClass.getSimpleName)
  val varName = varPrefix + Random.nextLong()

  //can only be overriden by lazy val
  //Python class must have a constructor that takes json format of its Scala class
  def constructPython(session: Session): String = {
    // java & python have different namespace convention.
    val pyClassName = this.getClass.getCanonicalName.stripPrefix("com.tribbloids.")
    val result = session.pythonDriver.interpret(
      s"""
         |$varName = $pyClassName('${this.toJSON}')
           """.trim.stripMargin
    )
    varName
  }

  def destructPython(session: Session): Unit = {
    val result = session.pythonDriver.interpret(
      s"""
         |del $varName
           """.trim.stripMargin
    )
    Unit
  }

  case class Py(session: Session) extends Dynamic {

    def applyDynamic(methodName: String)(args: Any*): Seq[String] = {
      val argJSONs = args.map {
        v =>
          MessageWrapper(v).compactJSON()
      }
      val result = session.pythonDriver.interpret(
        //self & session JSON is always the first & second params.
        s"""
           |$varName.$methodName(${SessionRelay.toMessage(session).compactJSON}, ${this})
           """.trim.stripMargin
      )

      result
    }
  }

  /**
    * must have exactly the same class/function under the same package imported in python that takes 2 JSON strings
    * 1 for action, 1 for session
    */
  override def exe(session: Session): Seq[Fetched] = {

    constructPython(session)

    try {
      super.exe(session)
    }
    finally {
      destructPython(session)
    }
  }
}