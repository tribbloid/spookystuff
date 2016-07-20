package com.tribbloids.spookystuff.extractors

import java.lang.reflect.Method

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType
import ScalaReflection.universe._
import org.apache.spark.ml.dsl.utils.FlowUtils
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

//2 stages plan:
// first, handle all ArgsOpt as Values
// second, handle ArgsOpt that can be other GenExtractor[T, _]
case class ScalaDynamicExtractor[T](
                                     base: GenExtractor[T, _],
                                     methodName: String,
                                     argsOpt: Option[List[GenExtractor[T, _]]]
                                   ) extends GenExtractor[T, Any] {

  import org.apache.spark.sql.TypeUtils._
  import Implicits._

  implicit def typeToEvidence(dataType: DataType): TypeEvidence = TypeEvidence(dataType)

  //only used to show TreeNode
  override protected def _args: Seq[GenExtractor[_, _]] = Seq(base) ++ argsOpt.toList.flatten

  def getMethodsByName(evi: TypeEvidence): List[MethodSymbol] = {
    val tpe = evi.baseScalaType.tpe

    //Java reflection preferred as more battle tested?
    val allMembers = tpe
      .members.toList

    val members = allMembers
      .filter(_.name.decoded == methodName)
      .map {
        v =>
          v.asMethod
      }

    members
  }

  def getExpectedTypeCombinations(argEvisOpt: Option[List[TypeEvidence]]): Seq[Option[List[Type]]] = {
    val expectedTypeList: Seq[Option[List[Type]]] = argEvisOpt match {
      case Some(argEvis) =>
        val eviss = argEvis.map {
          v =>
            List(v.baseScalaType.tpe)
        }
        val cartesian = FlowUtils.cartesianProductList(eviss)
        cartesian.map(
          v => Some(v)
        )
      case None =>
        Seq(None)
    }
    expectedTypeList
  }

  //2 cases: argEvisOpt = None: call by .name
  // argEvisOpt = Some(List()) call by .name()
  def getMethodByScala(baseEvi: TypeEvidence, argEvisOpt: Option[List[TypeEvidence]]): MethodSymbol = {
    val methods = getMethodsByName(baseEvi)

    val expectedTypeCombinations: Seq[Option[List[Type]]] =
      getExpectedTypeCombinations(argEvisOpt)

    val valid = methods.flatMap {
      method =>
        val paramTypess_returnType: (List[List[Type]], Type) = {
          TypeUtils.getParameter_ReturnTypes(method, baseEvi.baseScalaType.tpe)
        }
        val actualTypess: List[List[Type]] = paramTypess_returnType._1
        val firstTypeOpt = actualTypess.headOption

        if (actualTypess.size > 1)
          None //no currying
        else {
          if (expectedTypeCombinations.exists(v => TypeUtils.fitIntoArgs(v, firstTypeOpt)))
            Some(method)
          else
            None
        }
    }
    assert(valid.size <= 1)
    valid.headOption.getOrElse{
      val errorStrs = expectedTypeCombinations.map {
        tt =>
          val argsStr = tt.map{
            t =>
              "(" + t.mkString(", ") + ")"
          }
            .getOrElse("")
          s"method ${baseEvi.baseScalaType.tpe}.$methodName$argsStr does not exist"
      }
      throw new UnsupportedOperationException(
        errorStrs.mkString("\n")
      )
    }
  }

  /**
    * due to type erasure, the java-based type validation in this function is much looser.
    * Should always validate by getMethodByScala to fail fast
    */
  def getMethodByJava(baseEvi: TypeEvidence, argEvisOpt: Option[List[TypeEvidence]]): Method = {

    val baseClz = baseEvi.baseScalaType.toClass

    val expectedClasssList: Seq[Option[List[Class[_]]]] = argEvisOpt match {
      case Some(argEvis) =>
        val eviss: List[List[Class[_]]] = argEvis.map {
          v =>
            List(v.baseScalaType.toClass) :+ classOf[Object]
        }
        val cartesian = FlowUtils.cartesianProductList(eviss)
        cartesian.map(
          v => Some(v)
        )
      case None =>
        Seq(None)
    }

    val encodedMethodName = (methodName: TermName).encoded
    val methods = expectedClasssList.flatMap {
      classs =>
        try {
          val method = baseClz.getMethod(encodedMethodName, classs.getOrElse(Nil): _*)
          Some(method)
        }
        catch {
          case e: NoSuchMethodException => None
        }
    }

    assert(methods.size <= 1)
    methods.headOption.getOrElse{
      val errorStrs = expectedClasssList.map {
        tt =>
          val argsStr = tt.map {
            t =>
              "(" + t.mkString(", ") + ")"
          }
            .getOrElse("")
          s"method $baseClz.$methodName$argsStr does not exist"
      }
      throw new UnsupportedOperationException(
        errorStrs.mkString("\n")
      )
    }
  }

  //  def getFn(baseEvi: TypeEvidence, argEviss: List[List[TypeEvidence]]): Option[T => Any]  = {
  //    val validMethod = getScalaReflectionMethod(baseEvi, argEviss)
  //
  //    val cm = rootMirror.reflectClass(baseEvi.scalaType.tpe.typeSymbol.asClass)
  //    val mm =
  //
  //  }

  //resolve to a Spark SQL DataType according to an exeuction plan
  override def resolveType(tt: DataType): DataType = {
    val resultTag: _root_.org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag[Any] = _resolveTypeTag(tt)

    TypeEvidence(resultTag).catalystType
  }

  private def _resolveTypeTag(tt: DataType): ScalaReflection.universe.TypeTag[Any] = {
    //TODO: merge
    val baseEvi: TypeEvidence = TypeEvidence(base.resolveType(tt))
    val argEvis = argsOpt.map {
      _.map {
        v =>
          v.resolveType(tt): TypeEvidence
      }
    }
    val scalaMethod = getMethodByScala(baseEvi, argEvis)
    val (_, resultType) = TypeUtils.getParameter_ReturnTypes(scalaMethod, baseEvi.baseScalaType.tpe)
    val resultTag = TypeUtils.typeToTypeTag[Any](resultType, baseEvi.baseScalaType.mirror)
    resultTag
  }

  override def resolve(tt: DataType): PartialFunction[T, Any] = {
    val evi = _resolveTypeTag(tt)
    val resolvedOpt = resolveUsingJava(tt)

    val lifted = if (evi.tpe <:< typeOf[Option[Any]]) {
      resolvedOpt.andThen(
        v =>
          v.asInstanceOf[Option[Option[Any]]].flatten
      )
    }
    else resolvedOpt

    Unlift(lifted)
  }

  /**
    * will not be used due to bad performance
    */
  def resolveUsingScala(tt: DataType): T => Option[Any] = {
    val baseEvi: TypeEvidence = TypeEvidence(base.resolveType(tt))
    val argEvis = argsOpt.map {
      _.map {
        v =>
          v.resolveType(tt): TypeEvidence
      }
    }

    val baseLift: (T) => Option[Any] = base.resolve(tt).lift
    val argLifts: Option[List[(T) => Option[Any]]] = argsOpt.map(
      _.map(_.resolve(tt).lift)
    )

    val scalaMethod = getMethodByScala(baseEvi, argEvis)
    val baseMirror = baseEvi.baseScalaType.mirror
    val result = {
      vv: T =>
        val baseOpt = baseLift.apply(vv)
        val argOpts = argLifts.map(
          _.map(_.apply(vv))
        )
          .getOrElse(Nil)
        baseOpt.map {
          baseVal =>
            val instanceMirror = baseMirror.reflect(baseVal)
            val methodMirror = instanceMirror.reflectMethod(scalaMethod)
            methodMirror.apply(argOpts.map(_.get): _*)
        }
    }
    result
  }

  /**
    * for performance test, keep it simple
    * use base/args.resolve.lift to get arg values, use them if none of them is None
    * extend to handle None case in the future
    */
  def resolveUsingJava(tt: DataType): T => Option[Any] = {
    val baseEvi: TypeEvidence = TypeEvidence(base.resolveType(tt))
    val baseLift: (T) => Option[Any] = base.resolve(tt).lift

    val argEvisOpt: Option[List[TypeEvidence]] = argsOpt.map {
      _.map {
        v =>
          v.resolveType(tt): TypeEvidence
      }
    }
    //    val scalaMethod = this.getMethodByScala(baseEvi, argEvisOpt)
    //    val argLifts: List[(T) => Option[Any]] = argsOpt.map {
    //      args =>
    //        val argTypeEvis = argTypeEvisOpt.get
    //        val baseArgTypes = argTypeEvis.map(_.baseScalaType.tpe)
    //        val (argTypess, resultType) =
    //          TypeUtils.getParameter_ReturnTypes(scalaMethod, baseEvi.baseScalaType.tpe)
    //
    //        val argTypes = argTypess.head
    //
    //        val needLifting = argTypeEvis.zip(argTypes).map {
    //          tuple =>
    //            assert(tuple._1.scalaTypes.contains(tuple._2))
    //
    //            if (tuple._1.baseScalaType.tpe == tuple._2)
    //            else
    //        }
    //
    //        args.map(_.resolve(tt).lift)
    //
    //
    //    }
    //      .getOrElse(Nil)
    //
    //    val (argTypess, resultType) = TypeUtils.methodSymbolToParameter_ReturnTypes(scalaMethod, baseEvi.baseScalaType.tpe)

    val argLifts: List[(T) => Option[Any]] = argsOpt.map {
      _.map {
        v =>
          v.resolve(tt).lift
      }
    }
      .getOrElse(Nil)

    val javaMethod = this.getMethodByJava(baseEvi, argEvisOpt)

    //TODO: clean up this: useless
    val javaArgTypes = javaMethod.getParameterTypes
    val zipped = argLifts.zip(javaArgTypes)
    val effectiveArgs: List[(T) => Any] = zipped.map {
      tuple =>
        if (classOf[Option[Any]] isAssignableFrom tuple._2)
          tuple._1
        else
          Unlift(tuple._1)
    }

    val result = new Function1[T, Option[Any]] {

      override def apply(vv: T): Option[Any] = {
        val baseOpt = baseLift.apply(vv)
        baseOpt.flatMap {
          baseVal =>
            try {
              val result = javaMethod.invoke(
                baseVal,
                effectiveArgs.map(_.apply(vv).asInstanceOf[Object]): _*
              )
              Option(result) //TODO: handle option output!
            }
            catch {
              case e: MatchError =>
                None
            }
        }
      }
    }

    result
  }
}

/**
  * this complex mixin enables many scala functions of Docs & Unstructured to be directly called on Extraction shortcuts.
  * supersedes many implementations
  */
trait ScalaDynamicMixin[T, +R] extends Dynamic {
  selfType: GenExtractor[T, R] =>

  def selectDynamic(methodName: String): GenExtractor[T, Any] = {

    ScalaDynamicExtractor(this, methodName, None)
  }

  def applyDynamic(methodName: String)(args: Any*): GenExtractor[T, Any] = {

    val argExs: Seq[GenExtractor[T, Any]] = args.toSeq.map {
      case ex: GenExtractor[T, Any] =>
        ex
      case v@_ =>
        val dt = TypeEvidence.fromInstance(v).catalystType
        new GenLiteral[T, Any](Option(v), dt)
    }

    ScalaDynamicExtractor(this, methodName, Some(argExs.toList))
  }
}