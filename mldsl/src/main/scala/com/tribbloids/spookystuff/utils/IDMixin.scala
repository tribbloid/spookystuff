package com.tribbloids.spookystuff.utils

trait IDMixin {

  def _id: Any

  @transient lazy val idChecked = {
    val id = _id
    val effectiveID = id match {
      case aa: Array[_] => aa.toList
      case _ => id
    }
    effectiveID
  }

  final override def hashCode: Int = idChecked.##
  final override def equals(v: Any): Boolean = {
    if (v == null) false
    else if (v.isInstanceOf[AnyRef] && this.eq(v.asInstanceOf[AnyRef])) true
    else if (v.isInstanceOf[IDMixin]) { //TODO: should subclass be allowed to == this?
      v.asInstanceOf[IDMixin].idChecked == this.idChecked
    }
    else false
  }
}