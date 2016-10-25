
package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.Session
import org.apache.spark.ml.dsl.utils._

import scala.language.dynamics
import scala.util.Random

trait PyConverter {

  def scala2py(v: Any): (String, String) //import -> definition
}

object PyConverter {

  final val QQQ = "\"\"\""

  /**
    * convert scala object to python sourcecode
    * case class is convert to Python Constructor or variable
    * other types => JSON => Python types
    */
  object JSON extends PyConverter {

    // How deep does the recursion goes?
    // as deep as not inspecting case class constructor, if you do it all hell break loose
    // this limits class constructor to use only JSON compatible weak types, which is not a big deal.
    def scala2py(v: Any): (String, String) = {
      v match {
        case vv: PyObject =>
          val pyFullName = vv.pyClassNames.mkString(".")
          val pyPackage = vv.pyClassNames.slice(0, vv.pyClassNames.length - 1).mkString(".")
          //          val pyClass = pyFullName.last
          val json = vv.toMessage.prettyJSON
          val impt = s"import $pyPackage"
          val code =
            s"""
               |$pyFullName(**(json.loads(
               |$QQQ
               |$json
               |$QQQ
               |)))
               """.trim.stripMargin
          impt -> code
        case _ =>
          val json = MessageView(v).prettyJSON
          val code =
            s"""
               |json.loads(
               |$QQQ
               |$json
               |$QQQ
               |)
               """.trim.stripMargin
          "" -> code
      }
    }

    def args2py(vs: Iterable[Any]): (String, String) = {
      val pys = vs.map {
        v =>
          scala2py(v)
      }
      val impt = pys.map(_._1).mkString("\n")
      val code = pys.map(_._2).mkString(",")
      impt -> code
    }

    def kwargs2py(vs: Iterable[(String, Any)]): (String, String) = {
      val pys = vs.map {
        tuple =>
          val py = scala2py(tuple._2)
          py._1 -> (tuple._1 + "=" + py._2)
      }

      val impt = pys.map(_._1).mkString("\n")
      val code = pys.map(_._2).mkString(",")
      impt -> code
    }
  }
}

//this assumes that a Python class is always defined under pyspookystuff.
trait PyObject extends HasMessage {

  val pyClassNames: Array[String] = (
    "py" +
      this.getClass.getCanonicalName
        .split('.')
        .slice(2, Int.MaxValue)
        .filter(_.nonEmpty)
        .mkString(".")
    )
    .split('.')

  def converter = PyConverter.JSON

  //TODO: how to clean up variable? Should I prefer a stateless/functional design?
  val varPrefix = FlowUtils.toCamelCase(this.getClass.getSimpleName)
  val varName = varPrefix + Math.abs(Random.nextLong())

  //  object Py {
  //
  //    val
  //  }

  case class Py(session: Session) extends Dynamic {

    //Python class must have a constructor that takes json format of its Scala class
    def construct(): String = {

      val py = converter.scala2py(PyObject.this)
      val code =
        s"""
           |${py._1}
           |$varName = ${py._2}
       """.trim.stripMargin

      session.pythonDriver.interpret(code, Some(session))
      varName
    }

    lazy val constructed = construct()

    override def finalize(): Unit = {
      session.pythonDriver.interpret(
        s"del $varName",
        Some(session)
      )
      Unit
    }

    def applyDynamic(methodName: String)(args: Any*): String = {

      val py = converter.args2py(args)
      val code =
        s"""
           |${py._1}
           |result = $varName.$methodName(
           |${py._2}
           |)
      """.trim.stripMargin
      val result = session.pythonDriver.execute(code, sessionOpt = Some(session))._2

      result.getOrElse("Nil")
    }

    def applyDynamicNamed(methodName: String)(kwargs: (String, Any)*): String = {

      val py = converter.kwargs2py(kwargs)
      val code =
        s"""
           |${py._1}
           |result = $varName.$methodName(
           |${py._2}
           |)
      """.stripMargin
      val result = session.pythonDriver.execute(code, sessionOpt = Some(session))._2

      result.getOrElse("Nil")
    }
  }
}

trait PyAction extends Action with PyObject {

  /**
    * must have exactly the same class/function under the same package imported in python that takes 2 JSON strings
    * 1 for action, 1 for session
    */
  final override def exe(session: Session): Seq[Fetched] = {
    val py = this.Py(session)
    withLazyDrivers(session){
      py.construct()
      try {
        doExe(session)
      }
      finally {
        py.finalize()
      }
    }
  }
}