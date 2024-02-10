package com.tribbloids.spookystuff.utils.refl

//TODO: simply by using a common relay that different type representation can be cast into
/**
  * all implementation has to be synchronized and preferrably not executed concurrently to preserve efficiency.
  */
object ReflectionUtils {

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  // TODO: move most of them to ScalaType
  def getCaseAccessorSymbols(tt: TypeMagnet[_]): List[MethodSymbol] = {
    val accessors = tt.asType.members.toList.reverse
      .flatMap(filterCaseAccessors)
    accessors
  }

  def filterCaseAccessors(s: Symbol): Seq[MethodSymbol] = {
    s match {
      case m: MethodSymbol if m.isCaseAccessor =>
        Seq(m)
      case t: TermSymbol =>
        t.overrides.flatMap(filterCaseAccessors)
      case _ =>
        Nil
    }
  }

  def getCaseAccessorFields(tt: TypeMagnet[_]): List[(String, Type)] = {
    getCaseAccessorSymbols(tt).map { ss =>
      ss.name.decodedName.toString -> ss.typeSignature
    }
  }

  def getConstructorParameters(tt: TypeMagnet[_]): Seq[(String, Type)] = {
    val formalTypeArgs = tt.asType.typeSymbol.asClass.typeParams
    val TypeRef(_, _, actualTypeArgs) = tt.asType
    val constructorSymbol = tt.asType.member(termNames.CONSTRUCTOR)
    val params = if (constructorSymbol.isMethod) {
      constructorSymbol.asMethod.paramLists
    } else {
      // Find the primary constructor, and use its parameter ordering.
      val primaryConstructorSymbol: Option[Symbol] =
        constructorSymbol.asTerm.alternatives.find(s => s.isMethod && s.asMethod.isPrimaryConstructor)
      if (primaryConstructorSymbol.isEmpty) {
        sys.error("Internal SQL error: Product object did not have a primary constructor.")
      } else {
        primaryConstructorSymbol.get.asMethod.paramLists
      }
    }

    params.flatten.map { p =>
      p.name.toString -> p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
    }
  }

  def getCaseAccessorMap(v: Product): List[(String, Any)] = {
    val tt = TypeMagnet.FromClass(v.getClass)
    val ks = getCaseAccessorFields(tt).map(_._1)
    val vs = v.productIterator.toList
    assert(ks.size == vs.size)
    ks.zip(vs)
  }

  //    def newCase[A]()(implicit t: ClassTag[A]): A = {
  //      val cm = rootMirror
  //      val clazz = cm classSymbol t.runtimeClass
  //      val modul = clazz.companionSymbol.asModule
  //      val im = cm reflect (cm reflectModule modul).instance
  //      ReflectionUtils.invokeStatic(clazz)
  //      defaut[A](im, "apply")
  //    }
}
