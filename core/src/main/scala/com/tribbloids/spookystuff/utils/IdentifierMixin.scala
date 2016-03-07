package com.tribbloids.spookystuff.utils

trait IdentifierMixin {

  def _id: Any

  final override def hashCode: Int = _id.hashCode
  final override def equals(v: Any): Boolean = {
    if (v == null) false
    else if (v.isInstanceOf[AnyRef] && this.eq(v.asInstanceOf[AnyRef])) true
    else if (v.getClass.isAssignableFrom(this.getClass)) { //TODO: should subclass be allowed to == this?
      v.asInstanceOf[IdentifierMixin]._id == this._id
    }
    else false
  }
}