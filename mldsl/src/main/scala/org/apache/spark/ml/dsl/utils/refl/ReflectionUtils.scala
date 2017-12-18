package org.apache.spark.ml.dsl.utils.refl

//TODO: simply by using a common relay that different type representation can be cast into
/**
  * all implementation has to be synchronized and preferrably not executed concurrently to preserve efficiency.
  */
object ReflectionUtils extends ReflectionLock {

  import org.apache.spark.sql.catalyst.ScalaReflection.universe._

  //TODO: move most of them to ScalaType
  def getCaseAccessorSymbols(tt: ScalaType[_]): List[MethodSymbol] = locked{
    val accessors = tt.asType.members
      .toList
      .reverse
      .flatMap(filterCaseAccessors)
    accessors
  }

  def filterCaseAccessors(s: Symbol): Seq[MethodSymbol] = {
    s match {
      case m: MethodSymbol if m.isCaseAccessor =>
        Seq(m)
      case t: TermSymbol =>
        t.allOverriddenSymbols.flatMap(filterCaseAccessors)
      case _ =>
        Nil
    }
  }

  def getCaseAccessorFields(tt: ScalaType[_]): List[(String, Type)] = {
    getCaseAccessorSymbols(tt).map {
      ss =>
        ss.name.decoded -> ss.typeSignature
    }
  }

  def getConstructorParameters(tt: ScalaType[_]): Seq[(String, Type)] = locked{
    val formalTypeArgs = tt.asType.typeSymbol.asClass.typeParams
    val TypeRef(_, _, actualTypeArgs) = tt.asType
    val constructorSymbol = tt.asType.member(nme.CONSTRUCTOR)
    val params = if (constructorSymbol.isMethod) {
      constructorSymbol.asMethod.paramss
    } else {
      // Find the primary constructor, and use its parameter ordering.
      val primaryConstructorSymbol: Option[Symbol] = constructorSymbol.asTerm.alternatives.find(
        s => s.isMethod && s.asMethod.isPrimaryConstructor)
      if (primaryConstructorSymbol.isEmpty) {
        sys.error("Internal SQL error: Product object did not have a primary constructor.")
      } else {
        primaryConstructorSymbol.get.asMethod.paramss
      }
    }

    params.flatten.map { p =>
      p.name.toString -> p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
    }
  }

  def getCaseAccessorMap(v: Product): List[(String, Any)] = {
    val tt = ScalaType.fromClass(v.getClass)
    val ks = getCaseAccessorFields(tt).map(_._1)
    val vs = v.productIterator.toList
    assert (ks.size == vs.size)
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