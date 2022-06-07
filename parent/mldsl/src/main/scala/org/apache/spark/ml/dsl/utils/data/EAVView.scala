package org.apache.spark.ml.dsl.utils.data

import scala.util.Try

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

  lazy val withDefaults: EAVCore = {
    val declaredMap: Seq[(this.Attr[_], Any)] = declaredAttrs.flatMap { attr =>
      attr.get.map { v =>
        attr -> v
      }
    }

    val declaredEAV = EAV.fromMap(declaredMap)

    this :++ declaredEAV
  }

  /**
    * this will drop undeclared attrs & convert each name into primary name
    */
  lazy val canonical: EAVCore = {
    val declaredMap = declaredAttrs.flatMap { attr =>
      Try(attr.explicitValue).toOption.map { v =>
        attr -> v
      }
    }

    val declaredEAV = EAV.fromMap(declaredMap)

    declaredEAV.core
  }
}
