package com.tribbloids.spookystuff.extractors

import java.lang.reflect.Method

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType
import ScalaReflection.universe._
import org.apache.spark.sql.TypeUtils

import scala.language.{dynamics, implicitConversions}

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
                                     argss: List[List[GenExtractor[T, _]]]
                                   ) extends GenExtractor[T, Any] {

  import org.apache.spark.sql.TypeUtils._
  import Implicits._

  implicit def typeToEvidence(dataType: DataType): TypeEvidence = TypeEvidence(dataType)

  override protected def _args: Seq[GenExtractor[_, _]] = Seq(base) ++ argss.flatten

  def fnSymbol(tt: DataType) = {
    val baseEvi: TypeEvidence = base.resolveType(tt)
    val argsEvis: Seq[Seq[TypeEvidence]] = argss.map {
      args =>
        args.map {
          arg =>
            arg.resolveType(tt): TypeEvidence
        }
    }
    val baseParamss = _getAllMethods(baseEvi)
    //    val filtered
  }

  def _getAllMethods(evi: TypeEvidence): List[MethodSymbol] = {
    val tpe = evi.scalaType.tpe

    val symbol = tpe.typeSymbol

    //Java reflection preferred as more battle tested?
    val members = tpe
      .members.toList
      .filter(_.name.decoded == methodName)
      .map {
        v =>
          v.asMethod
      }

    members
  }

  def getScalaReflectionMethod(baseEvi: TypeEvidence, argEviss: List[List[TypeEvidence]]): Option[MethodSymbol] = {
    val methods = _getAllMethods(baseEvi)

    val valid = methods.filter {
      symbol =>
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

        val paramTypess_returnType = TypeUtils.methodSymbolToParameter_Returntypes(symbol, baseEvi.scalaType.tpe)
        val actualTypess: List[List[Type]] = paramTypess_returnType._1

        expectedTypess == actualTypess
    }
    assert(valid.size <= 1)
    valid.headOption
  }

  def getJavaReflectionMethods(baseEvi: TypeEvidence, argEviss: List[List[TypeEvidence]]): Method = {
    val baseClz = baseEvi.scalaType.toClass

    assert(argEviss.size == 1, "currying for java method is not supported")
    val argClzs = argEviss.flatten.map(
      _.scalaType.toClass//.asInstanceOf[Class[_]]
    )

    val method = baseClz.getMethod(methodName, argClzs: _*)
    method
  }

  //  def getFn(baseEvi: TypeEvidence, argEviss: List[List[TypeEvidence]]): Option[T => Any]  = {
  //    val validMethod = getScalaReflectionMethod(baseEvi, argEviss)
  //
  //    val cm = rootMirror.reflectClass(baseEvi.scalaType.tpe.typeSymbol.asClass)
  //    val mm =
  //
  //  }

  //resolve to a Spark SQL DataType according to an exeuction plan
  //TODO: test it immediately!
  override def resolveType(tt: DataType): DataType = {
    val baseEvi: TypeEvidence = TypeEvidence(base.resolveType(tt))
    val argEvis = argss.map {
      _.map {
        v =>
          v.resolveType(tt): TypeEvidence
      }
    }
    val symbol = getScalaReflectionMethod(baseEvi, argEvis).get
    val resultType = TypeUtils.methodSymbolToParameter_Returntypes(symbol, baseEvi.scalaType.tpe)._2
    val resultTag = TypeUtils.typeToTypeTag(
      resultType, baseEvi.scalaType.mirror
    )
    TypeEvidence(resultTag).catalystType
  }

  override def resolve(tt: DataType): PartialFunction[T, Any] = {

    ???
  }
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