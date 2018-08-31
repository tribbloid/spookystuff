package org.apache.spark.ml.dsl

import org.apache.spark.ml.dsl.utils.messaging.MessageReader
import org.apache.spark.ml.dsl.utils.{FallbackJSONSerializer, FlowUtils}
import org.apache.spark.ml.param.{Param, Params}

import scala.language.{dynamics, implicitConversions}
import scala.reflect.ClassTag

/**
  * Created by peng on 10/04/16.
  */
trait DynamicParamsMixin extends Params with Dynamic {

  implicit protected def unwrap[T](v: Param[T]): T = this.getOrDefault(v)

  def applyDynamic(methodName: String)(args: Any*): this.type = {

    if (methodName.startsWith("set")) {
      assert(args.length == 1)
      val arg = args.head

      val fieldName = methodName.stripPrefix("set")
      val expectedName = methodName.stripPrefix("set")
      val fieldOption =
        this.params.find(v => (v.name == expectedName) || (FlowUtils.liftCamelCase(v.name) == expectedName))

      fieldOption match {
        case Some(field) =>
          set(field.asInstanceOf[Param[Any]], arg)
        case None =>
          throw new IllegalArgumentException(s"parameter $fieldName doesn't exist")
        //          dynamicParams.put(fieldName, arg)
      }

      this
    } else throw new IllegalArgumentException(s"function $methodName doesn't exist")
  }

  protected def Param[T: ClassTag](
      name: String = FlowUtils.callerMethodName(),
      doc: String = "Pending ...",
      default: T = null
  ): Param[T] = {

    val result = new Param[T](this, name, doc)

    Option(default).foreach(v => this.setDefault(result, v))

    result
  }

  protected def GenericParam[T: Manifest](
      name: String = FlowUtils.callerMethodName(),
      doc: String = "Pending ...",
      default: T = null
  ): Param[T] = {

    val reader = new MessageReader[T]() {

      override def formats = super.formats + FallbackJSONSerializer
    }

    val result = reader.Param(this, name, doc)

    Option(default).foreach(v => this.setDefault(result, v))

    result
  }
}
