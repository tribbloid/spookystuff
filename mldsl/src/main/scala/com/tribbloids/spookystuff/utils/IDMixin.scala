package com.tribbloids.spookystuff.utils

trait IDMixin {

  def _id: Any

  @transient lazy val idChecked = {
    val id = _id
    require(!id.isInstanceOf[Array[_]], "IDMixin._id cannot be Array")
    id
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