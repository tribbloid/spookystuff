package com.tribbloids.spookystuff.utils

trait IDMixin extends HasID {

  override def hashCode: Int = id.##
  override def equals(v: Any): Boolean = {
    if (v == null) false
    else if (v.isInstanceOf[AnyRef] && this.eq(v.asInstanceOf[AnyRef])) true
    else if (v.isInstanceOf[IDMixin]) { //TODO: should subclass be allowed to == this?
      v.asInstanceOf[IDMixin].id == this.id
    } else false
  }
}

object IDMixin {

  def product2ID(v: Product): (String, List[Any]) = {

    v.productPrefix -> v.productIterator.toList
  }
}
