//package com.tribbloids.spookystuff.extractors
//
//import org.apache.spark.sql.types.DataType
//
//import scala.language.dynamics
//
////TODO: major revision! function should be pre-determined by
//object ScalaDynamic {
//
//  //TODO: type erasure! add ClassTag
//  def invokeDynamically[T, R](
//                               v1: T,
//                               lifted: T => Option[R],
//                               methodName: String
//                             )(
//                               args: Any*
//                             ): Option[Any] = {
//
//    val selfValue: R = lifted.apply(v1).getOrElse(return None)
//
//    val argValues: Seq[Any] = args.map {
//
//      case expr: GenExtractor[T, Any] =>
//        val result = expr.lift.apply(v1)
//        if (result.isEmpty) return None
//        else result.get
//      case v@_ => v
//    }
//
//    val argClasses = argValues.map(_.getClass)
//
//    val func = selfValue.getClass.getMethod(methodName, argClasses: _*)
//
//    val result = func.invoke(selfValue, argValues.map(_.asInstanceOf[Object]): _*)
//    Some(result)
//  }
//}
//
//case class ScalaDynamic[T, R](
//                          base: GenExtractor[T, R],
//                          methodName: String,
//                          args: Seq[Any]
//                        ) extends Unlift[T, Any] {
//
//  override def liftApply(v1: T): Option[Any] =
//    ScalaDynamic.invokeDynamically[T, R](v1, base.lift, methodName)(args: _*)
//
//  // new feature used for direct schema generation
//  //resolve to a Spark SQL DataType according to an exeuction plan
//  override def applyType(tt: DataType): DataType = base.applyType(tt)
//}
//
///**
//  * this complex mixin enables many scala functions of Docs & Unstructured to be directly called on Extraction shortcuts.
//  * supersedes many implementations
//  */
//trait ScalaDynamicMixin[T, +R] {
//  selfType: GenExtractor[T, R] =>
//
//  def applyDynamic(methodName: String)(args: Any*): GenExtractor[T, Any] =
//    ScalaDynamic(this, methodName, args)
//}