package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.PipelineException
import com.tribbloids.spookystuff.utils.Utils
import org.apache.spark.ml.param.{Param, Params}

import scala.language.{dynamics, implicitConversions}
import scala.reflect.ClassTag

/**
  * Created by peng on 10/04/16.
  */
trait DynamicParamsMixin extends Params with Dynamic {

  implicit protected def unwrap[T](v: Param[T]): T = this.getOrDefault(v)

  def liftCamelcase(str: String) = str.head.toUpper.toString + str.substring(1)

  def applyDynamic(methodName: String)(args: Any*): this.type = {

    if (methodName.startsWith("set")) {
      assert(args.length == 1)
      val arg = args.head

      val fieldName = methodName.stripPrefix("set")
      val expectedName = methodName.stripPrefix("set")
      val fieldOption = this.params.find(v => (v.name == expectedName) || (liftCamelcase(v.name) == expectedName))

      fieldOption match {
        case Some(field) =>
          set(field.asInstanceOf[Param[Any]], arg)
        case None =>
          throw new PipelineException(s"parameter $fieldName doesn't exist")
        //          dynamicParams.put(fieldName, arg)
      }

      this
    }
    else throw new PipelineException(s"function $methodName doesn't exist")
  }

  protected def Param[T: ClassTag](
                                    name: String = {
                                      val bp = Utils.breakpoint().apply(2)
                                      assert(!bp.isNativeMethod) //can only use default value in def & lazy val blocks
                                      bp.getMethodName
                                    },
                                    doc: String = "Pending ...",
                                    default: T = null
                                  ): Param[T] = {

    val result = new Param[T](this, name, doc)

    Option(default).foreach(v => this.setDefault(result, v))

    result
  }

  //TODO: need debugging
  protected def SerializingParam[T: ClassTag](
                                               name: String = {
                                                 val bp = Utils.breakpoint().apply(2)
                                                 assert(!bp.isNativeMethod) //can only use default value in def & lazy val blocks
                                                 bp.getMethodName
                                               },
                                               doc: String = "Pending ...",
                                               default: T = null
                                             ): Param[T] = {

    val result = new JavaSerializationParam[T](this, name, doc)

    Option(default).foreach(v => this.setDefault(result, v))

    result
  }
}
