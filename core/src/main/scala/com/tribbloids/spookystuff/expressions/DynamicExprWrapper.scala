package com.tribbloids.spookystuff.expressions

import com.tribbloids.spookystuff.row.PageRow

import scala.language.dynamics

/**
 * Created by peng on 01/11/15.
 */
class DynamicExprWrapper[+T](val self: Expression[T]) extends Serializable with Dynamic {

  def applyDynamic(methodName: String)(args: Any*): Expression[Any] = {

    val resultFn: (PageRow => Option[Any]) = {
      v1 =>
        val selfOption: Option[T] = self.apply(v1)
        selfOption.map {
          selfValue =>
            val argValues: Seq[Any] = args.map {
              case dynamic: DynamicExprWrapper[Any] => dynamic.self.apply(v1).orNull: Any
              case arg @ _ => arg: Any
            }
            val argClasses = argValues.map(_.getClass)

            val func = selfValue.getClass.getMethod(methodName, argClasses: _*)

            //        func.getReturnType
            func.invoke(selfValue, argValues.map(_.asInstanceOf[Object]): _*)
        }
    }

    val argNames = args map {
      case dynamic: DynamicExprWrapper[Any] => dynamic.self.name
      case arg @ _ => "" + arg
    }

    val argStr = argNames.mkString(",")
    val resultName = self.name + "." + methodName + "(" + argStr + ")"

    ExpressionLike(resultFn, resultName)
  }
}
