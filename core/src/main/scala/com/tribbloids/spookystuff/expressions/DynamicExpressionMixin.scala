package com.tribbloids.spookystuff.expressions

import scala.language.dynamics

object DynamicExpressionLike {

  //TODO: add ClassTag
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

case class DynamicExpressionLike[T](
                                     base: ExpressionLike[T, _],
                                     methodName: String,
                                     args: Seq[Any]
                                   ) extends UnliftedExpressionLike[T, Any] {

  override def liftApply(v1: T): Option[Any] =
    DynamicExpressionLike.combineDynamically(v1, base.lift, methodName)(args: _*)
}

//extra functions
trait DynamicExpressionMixin[T, +R] extends Dynamic {
  selfType: ExpressionLike[T, R] =>

  def applyDynamic(methodName: String)(args: Any*): ExpressionLike[T, Any] =
    DynamicExpressionLike(this, methodName, args)
}