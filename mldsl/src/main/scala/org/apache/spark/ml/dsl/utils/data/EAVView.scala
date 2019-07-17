package org.apache.spark.ml.dsl.utils.data

trait EAVView extends EAV {

  lazy val declaredAttrs: List[Attr[_]] = {

    val methods = this.getClass.getMethods.toList
      .filter { method =>
        val parameterMatch = method.getParameterCount == 0
        val returnTypeMatch = classOf[Attr[_]].isAssignableFrom(method.getReturnType)

        returnTypeMatch && parameterMatch
      }

    val publicMethods = methods.filter { method =>
      method.getModifiers == 1
    }

    publicMethods.map { method =>
      method.invoke(this).asInstanceOf[Attr[_]]
    }
  }

  lazy val dropUndeclared: EAVCore = dropAll(declaredAttrs)

  lazy val withDefaults: EAVCore = {
    val declaredMap: Seq[(this.Attr[_], Any)] = declaredAttrs.flatMap { k =>
      k.get.map { v =>
        k -> v
      }
    }

    val declaredEAV = EAV.fromMap(declaredMap)

    this :++ declaredEAV
  }
}
