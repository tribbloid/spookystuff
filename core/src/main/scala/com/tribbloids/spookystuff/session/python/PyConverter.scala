package com.tribbloids.spookystuff.session.python

import org.apache.spark.ml.dsl.utils._

/**
  * Created by peng on 01/11/16.
  */
trait PyConverter {

  def scala2py(v: Any): (Seq[PyRef], String) //preprocessing -> definition

  def scala2ref(v: Any): (Seq[PyRef], String) = {
    v match {
      case vv: PyRef =>
        Seq(vv) -> vv.referenceOpt.get
      case _ =>
        scala2py(v)
    }
  }

  def args2Ref(vs: Iterable[Any]): (Seq[PyRef], String) = {
    val pys = vs.map {
      v =>
        scala2ref(v)
    }
    val deps = pys.map(_._1).reduceOption(_ ++ _).getOrElse(Nil)
    val code =
      s"""
         |(
         |${pys.map(_._2).mkString(", ")}
         |)
         """.trim.stripMargin
    deps -> code
  }

  def kwargs2Ref(vs: Iterable[(String, Any)]): (Seq[PyRef], String) = {
    val pys = vs
      .filter{
        tuple =>
          val v = tuple._2
          (v != null) && (v != None)
      }
      .map {
        tuple =>
          val py = scala2ref(tuple._2)
          py._1 -> (tuple._1 + "=" + py._2)
      }

    val deps = pys.map(_._1).reduceOption(_ ++ _).getOrElse(Nil)
    val code =
      s"""
         |(
         |${pys.map(_._2).mkString(", ")}
         |)
         """.trim.stripMargin
    deps -> code
  }
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
    def scala2py(v: Any): (Seq[PyRef], String) = {
      val json = v match {
        case vv: HasMessage =>
          vv.toMessage.prettyJSON
        case _ =>
          MessageView(v).prettyJSON
      }
      val code =
        s"""
           |json.loads(
           |$QQQ
           |$json
           |$QQQ
           |)
               """.trim.stripMargin
      Nil -> code
    }
  }
}