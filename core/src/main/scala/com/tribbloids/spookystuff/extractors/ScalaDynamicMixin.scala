package com.tribbloids.spookystuff.extractors

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType

import ScalaReflection.universe._

import scala.language.dynamics

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

case class ScalaDynamicExtractor[T](
                                     base: GenExtractor[T, _],
                                     methodName: String,
                                     argss: Seq[Seq[GenExtractor[T, _]]]
                                   ) extends GenExtractor[T, Any] {

  override protected def _args: Seq[GenExtractor[_, _]] = Seq(base) ++ argss.flatten

  def fnSymbol(tt: DataType) = {
    val baseEvi = TypeEvidence(base.resolveType(tt))
    val argsEvis = argss.map {
      args =>
        args.map {
          arg =>
            TypeEvidence(arg.resolveType(tt))
        }
    }
    val baseParamss = getMethods(baseEvi)
    //    val filtered
  }

  def getMethods(evi: TypeEvidence): List[MethodSymbol] = {
    val baseType = evi.scalaTypeOpt.getOrElse {
      throw new UnsupportedOperationException(s"base type $evi cannot be resolved")
    }

    //Java reflection preferred as more battle tested?
    val members = baseType
      .tpe
      .member(methodName: TermName)
      .asTerm
      .alternatives
      .map(_.asMethod)

    members
  }

  def getValidMethod(baseEvi: TypeEvidence, argEviss: List[List[TypeEvidence]]): Option[MethodSymbol] = {
    val methods = getMethods(baseEvi)

    val valid = methods.find {
      method =>
        val expectedTypess: List[List[Type]] = argEviss.map {
          argEvis =>
            argEvis.map {
              argEvi =>
                val ttg = argEvi.scalaTypeOpt.getOrElse {
                  throw new UnsupportedOperationException(s"argument type $baseEvi cannot be resolved")
                }
                ttg.tpe
            }
        }

        val paramss = method.paramss
        val actualTypess: List[List[Type]] = paramss.map {
          params =>
            params.map {
              param =>
                param.typeSignature
            }
        }

        expectedTypess == actualTypess
    }
    valid
  }

  //resolve to a Spark SQL DataType according to an exeuction plan
  override def resolveType(tt: DataType): DataType = ???

  override def resolve(tt: DataType): PartialFunction[T, Any] = ???
}
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