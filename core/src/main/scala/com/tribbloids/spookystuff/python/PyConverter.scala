package com.tribbloids.spookystuff.python

import com.tribbloids.spookystuff.python.ref.PyRef
import org.apache.spark.ml.dsl.utils.messaging.Registry

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

  //won't take missing parameter
  def args2Ref(vs: Iterable[Any]): (Seq[PyRef], String) = {
    val pys = vs.map { v =>
      scala2ref(v)
    }
    val deps = pys.map(_._1).reduceOption(_ ++ _).getOrElse(Nil)
    val code =
      s"""
         |(${pys.map(_._2).mkString(", ")})
         """.trim.stripMargin
    deps -> code
  }

  //takes optional parameter, treat as missing if value of the option is None
  def kwargs2Code(vs: Iterable[(String, Any)]): (Seq[PyRef], String) = {
    val pys: Iterable[(Seq[PyRef], String)] = vs
      .flatMap { tuple =>
        tuple._2 match {
          case opt: Option[Any] =>
            opt.map(v => tuple._1 -> v)
          case _ =>
            Some(tuple)
        }
      }
      .map { tuple =>
        val py = scala2ref(tuple._2)
        py._1 -> (tuple._1 + "=" + py._2)
      }

    val deps = pys.map(_._1).reduceOption(_ ++ _).getOrElse(Nil)
    val code =
      s"""
         |(${pys.map(_._2).mkString(", ")})
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
      val codec = Registry.Default.findCodecOrDefault(v)
      val json = codec.toWriter_>>(v).prettyJSON

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
