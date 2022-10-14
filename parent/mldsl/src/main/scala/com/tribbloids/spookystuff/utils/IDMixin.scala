package com.tribbloids.spookystuff.utils

// TODO: renamed to EqualityFix
trait IDMixin {

  import IDMixin._

  // TODO: renamed to equalityID
  protected def _id: Any
  @transient final private lazy val id: Any = {
    idCompose(_id)
  }

  override def hashCode: Int = id.##
  override def equals(v: Any): Boolean = {
    if (v == null) false
    else if (v.isInstanceOf[AnyRef] && this.eq(v.asInstanceOf[AnyRef])) true
    else if (v.isInstanceOf[IDMixin]) { // TODO: should subclass be allowed to == this?
      (v.getClass == this.getClass) &&
      (v.asInstanceOf[IDMixin].id == this.id)
    } else false
  }
}

object IDMixin {

  trait ForProduct extends IDMixin with Product {

    @transient override protected lazy val _id: (String, List[Any]) = productPrefix -> productIterator.toList
  }

  def idCompose(id: Any): Any = {
    val result = id match {
      case aa: Array[_] => aa.toList
      case _            => id
    }
    result
  }

  def product2ID(v: Product): (String, List[Any]) = {

    v.productPrefix -> v.productIterator.toList
  }
}
