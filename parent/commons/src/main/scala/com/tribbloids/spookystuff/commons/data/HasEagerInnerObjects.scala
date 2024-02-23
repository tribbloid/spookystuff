package com.tribbloids.spookystuff.commons.data

trait HasEagerInnerObjects {

  @volatile private var innerObjectsInitialised: Boolean = false

  protected def requireInitialised(): Unit = {
    require(innerObjectsInitialised, "initialisation not finished")
  }

  {
    declaredEagerInnerObjects
    innerObjectsInitialised = true
  }

  private lazy val declaredEagerInnerObjects: List[EagerInnerObject] = {

    val methods = this.getClass.getMethods.toList
      .filter { method =>
        val parameterMatch = method.getParameterCount == 0
        val returnTypeMatch = classOf[EagerInnerObject].isAssignableFrom(method.getReturnType)

        returnTypeMatch && parameterMatch
      }

    val publicMethods = methods.filter { method =>
      method.getModifiers == 1
    }

    publicMethods.map { method =>
      method.invoke(this).asInstanceOf[EagerInnerObject]
    }
  }

  trait EagerInnerObject {}
}
