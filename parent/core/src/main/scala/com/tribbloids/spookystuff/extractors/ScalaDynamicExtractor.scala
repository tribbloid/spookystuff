package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.commons.DSLUtils
import com.tribbloids.spookystuff.commons.refl.{CatalystTypeOps, TypeMagnet, TypeUtils, UnreifiedObjectType}
import org.apache.spark.sql.catalyst.ScalaReflection.universe._

import java.lang.reflect.Method

case class ScalaDynamic(
    methodName: String
) extends CatalystTypeOps.ImplicitMixin {

  def getMethodsByCatalystType(dType: DataType): List[MethodSymbol] = {
    val tpe = dType.typeTag_wild.tpe

    // Java reflection preferred as more battle tested?
    val allMembers = tpe.members.toList

    val members = allMembers
      .filter(_.name.decodedName.toString == methodName)
      .map { v =>
        v.asMethod
      }

    members
  }

  def getExpectedTypeCombinations(argDTypesOpt: Option[List[DataType]]): Seq[Option[List[Type]]] = {
    val expectedTypeList: Seq[Option[List[Type]]] = argDTypesOpt match {
      case Some(dTypes) =>
        val tpess = dTypes.map { v =>
          List(v.typeTag_wild.tpe)
        }
        val cartesian = DSLUtils.cartesianProductList(tpess)
        cartesian.map(v => Some(v))
      case None =>
        Seq(None)
    }
    expectedTypeList
  }

  // 2 cases: argDTypesOpt = None: call by .name
  // argDTypesOpt = Some(List()) call by .name()
  def getMethodByScala(baseDType: DataType, argDTypesOpt: Option[List[DataType]]): MethodSymbol = {
    val methods = getMethodsByCatalystType(baseDType)

    val expectedTypeCombinations: Seq[Option[List[Type]]] =
      getExpectedTypeCombinations(argDTypesOpt)

    val valid = methods.flatMap { method =>
      val paramTypess_returnType: (List[List[Type]], Type) = {
        TypeUtils.getParameter_ReturnTypes(method, baseDType.typeTag_wild.tpe)
      }
      val actualTypess: List[List[Type]] = paramTypess_returnType._1
      val firstTypeOpt = actualTypess.headOption

      if (actualTypess.size > 1)
        None // no currying
      else {
        if (expectedTypeCombinations.exists(v => TypeUtils.fitIntoArgs(v, firstTypeOpt)))
          Some(method)
        else
          None
      }
    }
    assert(valid.size <= 1)
    valid.headOption.getOrElse {
      val errorStrs = expectedTypeCombinations.map { tt =>
        val argsStr = tt
          .map { t =>
            "(" + t.mkString(", ") + ")"
          }
          .getOrElse("")
        s"method ${baseDType.typeTag_wild.tpe}.$methodName$argsStr does not exist"
      }
      throw new UnsupportedOperationException(
        errorStrs.mkString("\n")
      )
    }
  }

  /**
    * due to type erasure, the java-based type validation in this function is much looser. Should always validate by
    * getMethodByScala to fail fast
    */
  def getMethodByJava(baseDType: DataType, argDTypesOpt: Option[List[DataType]]): Method = {

    val baseClz = baseDType.magnet.asClass

    val expectedClasssList: Seq[Option[List[Class[_]]]] = argDTypesOpt match {
      case Some(argDTypes) =>
        val classess: List[List[Class[_]]] = argDTypes.map { v =>
          List(v.magnet.asClass) :+ classOf[Object]
        }
        val cartesian = DSLUtils.cartesianProductList(classess)
        cartesian.map(v => Some(v))
      case None =>
        Seq(None)
    }

    val encodedMethodName = TermName(methodName).encodedName.toString
    val methods = expectedClasssList.flatMap { classs =>
      try {
        val method = baseClz.getMethod(encodedMethodName, classs.getOrElse(Nil): _*)
        Some(method)
      } catch {
        case _: NoSuchMethodException => None
      }
    }

    assert(methods.size <= 1)
    methods.headOption.getOrElse {
      val errorStrs = expectedClasssList.map { tt =>
        val argsStr = tt
          .map { t =>
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
}

//2 stages plan:
// first, handle all ArgsOpt as Values
// second, handle ArgsOpt that can be other GenExtractor[T, _]
case class ScalaDynamicExtractor[T](
    base: GenExtractor[T, _],
    methodName: String,
    argsOpt: Option[List[GenExtractor[T, _]]]
) extends GenExtractor[T, Any] {

  val dynamic: ScalaDynamic = ScalaDynamic(methodName)

  // only used to show TreeNode
  override protected def _args: Seq[GenExtractor[_, _]] = Seq(base) ++ argsOpt.toList.flatten

  // resolve to a Spark SQL DataType according to an exeuction plan
  override def resolveType(tt: DataType): DataType = {
    val tag: TypeTag[Any] = _resolveTypeTag(tt)

    UnreifiedObjectType.summon(tag)
  }

  private def _resolveTypeTag(tt: DataType): TypeTag[Any] = {
    // TODO: merge
    val baseDType: DataType = base.resolveType(tt)
    val argDTypes = argsOpt.map {
      _.map { v =>
        v.resolveType(tt): DataType
      }
    }
    val scalaMethod: MethodSymbol = dynamic.getMethodByScala(baseDType, argDTypes)

    val baseTTg = baseDType.typeTag_wild

    val (paramTypes, resultType) = TypeUtils.getParameter_ReturnTypes(scalaMethod, baseTTg.tpe)

    val resultMagnet = TypeMagnet.fromType[Any](resultType, baseTTg.mirror)
    resultMagnet.asTypeTag
  }

  override def resolve(tt: DataType): PartialFunction[T, Any] = {
    val resolvedFn = resolveUsingScala(tt)

    val lifted = if (_resolveTypeTag(tt).tpe <:< typeOf[Option[Any]]) {
      resolvedFn.andThen(v => v.asInstanceOf[Option[Option[Any]]].flatten)
    } else resolvedFn

    Unlift(lifted)
  }

  def resolveUsingScala(tt: DataType): T => Option[Any] = {

    val baseLift: (T) => Option[Any] = base.resolve(tt).lift
    val argLifts: Option[List[(T) => Option[Any]]] = argsOpt.map(
      _.map(_.resolve(tt).lift)
    )

    ScalaResolvedFunction(
      this,
      tt,
      baseLift,
      argLifts
    )
  }

  /**
    * for performance test, keep it simple use base/args.resolve.lift to get arg values, use them if none of them is
    * None extend to handle None case in the future
    */
  def resolveUsingJava(tt: DataType): T => Option[Any] = {
    val baseDType: DataType = base.resolveType(tt)
    val baseLift: (T) => Option[Any] = base.resolve(tt).lift

    val argDTypesOpt: Option[List[DataType]] = argsOpt.map {
      _.map { v =>
        v.resolveType(tt): DataType
      }
    }
    val argLifts: List[(T) => Option[Any]] = argsOpt
      .map {
        _.map { v =>
          v.resolve(tt).lift
        }
      }
      .getOrElse(Nil)

    val javaMethod: Method = dynamic.getMethodByJava(baseDType, argDTypesOpt)

    val javaArgTypes = javaMethod.getParameterTypes
    val zipped = argLifts.zip(javaArgTypes)
    val effectiveArgs: List[(T) => Any] = zipped.map { tuple =>
      if (classOf[Option[Any]] isAssignableFrom tuple._2)
        tuple._1
      else
        Unlift(tuple._1)
    }

    val result = new Function1[T, Option[Any]] {

      override def apply(vv: T): Option[Any] = {
        val baseOpt = baseLift.apply(vv)
        baseOpt.flatMap { baseVal =>
          try {
            val result = javaMethod.invoke(
              baseVal,
              effectiveArgs.map(_.apply(vv).asInstanceOf[Object]): _*
            )
            Option(result) // TODO: handle option output!
          } catch {
            case _: MatchError =>
              None
          }
        }
      }
    }

    result
  }
}

case class ScalaResolvedFunction[T](
    extractor: ScalaDynamicExtractor[T],
    tt: DataType,
    baseLift: (T) => Option[Any],
    argLifts: Option[List[(T) => Option[Any]]]
) extends Function1[T, Option[Any]] {

  @transient lazy val baseDType: DataType = extractor.base.resolveType(tt)
  @transient lazy val argDTypes: Option[List[DataType]] = extractor.argsOpt.map {
    _.map { v =>
      v.resolveType(tt): DataType
    }
  }

  @transient lazy val scalaMethod: MethodSymbol = {
    extractor.dynamic.getMethodByScala(baseDType, argDTypes)
  }

  override def apply(vv: T): Option[Any] = {
    val baseOpt = baseLift.apply(vv)
    val argOpts = argLifts
      .getOrElse(Nil)
      .map(_.apply(vv))
    if (argOpts.contains(None)) None
    else {
      baseOpt.map { baseVal =>
        val loader = Option(baseVal)
          .map { _ =>
            baseVal.getClass.getClassLoader
          }
          .getOrElse(return None)
        val baseMirror = runtimeMirror(loader)

        val instanceMirror: InstanceMirror = baseMirror.reflect(baseVal)
        val methodMirror: MethodMirror = instanceMirror.reflectMethod(scalaMethod)
        methodMirror.apply(argOpts.map(_.get): _*)
      }
    }
  }
}
