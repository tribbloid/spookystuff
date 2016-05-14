package com.tribbloids.spookystuff.expressions

import scala.language.dynamics

object DynamicExpressionMixin {

  def combineDynamically[T, R](
                             v1: T,
                             lifted: T => Option[R],
                             methodName: String
                           )(
                             args: Any*
                           ): Option[Any] = {

    val selfOption: Option[R] = lifted.apply(v1)
    selfOption.map {
      selfValue =>
        val argValues: Seq[Any] = args.map {
          case expr: ExpressionLike[T, Any] =>
            val result = expr.lift.apply(v1)
            if (result.isEmpty) return None
            else result.get
          case v@_ => v
        }

        val argClasses = argValues.map(_.getClass)

        val func = selfValue.getClass.getMethod(methodName, argClasses: _*)

        func.invoke(selfValue, argValues.map(_.asInstanceOf[Object]): _*)
    }
  }
}

//extra functions
trait DynamicExpressionMixin[T, +R] extends Dynamic {
  selfType: ExpressionLike[T, R] =>

  def applyDynamic(methodName: String)(args: Any*): ExpressionLike[T, Any] = {

    val lifted: T => Option[Any] = {
      v1 =>
        DynamicExpressionMixin.combineDynamically(v1, this.lift, methodName)(args: _*)
    }

    new UnliftExpressionLike[T, Any] {
      override def liftApply(v1: T): Option[Any] = lifted(v1)
    }
  }
}