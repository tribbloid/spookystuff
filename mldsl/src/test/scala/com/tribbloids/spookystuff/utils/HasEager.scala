package com.tribbloids.spookystuff.utils

import java.lang.reflect.Modifier

/**
  * any inner object that extends `Eager` will be rendered immediately in their parent's constructor
  */
trait HasEager {

  trait Eager

  trait NOTEager

  def reifyEager(): Unit = {
//    val fields = this.getClass.getDeclaredFields.filter { ff =>
//      classOf[Eager].isAssignableFrom(ff.getType)
//    }

    val methods = this.getClass.getDeclaredMethods.filter { mm =>
      Modifier.isPublic(mm.getModifiers) &&
      mm.getParameterCount == 0 &&
      classOf[Eager].isAssignableFrom(mm.getReturnType) &&
      !classOf[NOTEager].isAssignableFrom(mm.getReturnType)
    }

//    for (ff <- fields) {
//      ff.setAccessible(true)
//      ff.get(this)
//    }

    for (mm <- methods) {
      mm.invoke(this)
    }
  }
}
