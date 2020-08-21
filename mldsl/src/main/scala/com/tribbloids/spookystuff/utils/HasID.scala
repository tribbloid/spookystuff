package com.tribbloids.spookystuff.utils

trait HasID {

  protected def _id: Any

  protected def _idCompose(id: Any): Any = {
    val result = _id match {
      case aa: Array[_] => aa.toList
      case _            => id
    }
    result
  }

  @transient final lazy val id: Any = {
    _idCompose(_id)
  }
}
