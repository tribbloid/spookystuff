package com.tribbloids.spookystuff.extractors

import scala.language.dynamics

object DynamicGenExtractor {

  //TODO: type erasure! add ClassTag
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

          case expr: GenExtractor[T, Any] =>
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

case class DynamicGenExtractor[T](
                                   base: GenExtractor[T, _],
                                   methodName: String,
                                   args: Seq[Any]
                                   ) extends UnliftedGenExtractor[T, Any] {

  override def liftApply(v1: T): Option[Any] =
    DynamicGenExtractor.combineDynamically(v1, base.lift, methodName)(args: _*)
}

//extra functions
trait DynamicHelper[T, +R] extends Dynamic {
  selfType: GenExtractor[T, R] =>

  def applyDynamic(methodName: String)(args: Any*): GenExtractor[T, Any] =
    DynamicGenExtractor(this, methodName, args)
}