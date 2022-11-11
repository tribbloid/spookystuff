package com.tribbloids.spookystuff.utils

trait EqualBy {

  import EqualBy._

  protected def _equalBy: Any
  @transient final private lazy val equalBy: Any = {
    rectifyArray(_equalBy)
  }

  override def hashCode: Int = equalBy.##
  override def equals(v: Any): Boolean = {
    if (v == null) false
    else
      v match {
        case value: AnyRef if this.eq(value) => true
        case by: EqualBy =>
          (v.getClass == this.getClass) &&
          (by.equalBy == this.equalBy)
        case _ => false
      }
  }
}

object EqualBy {

  trait FieldsWithTolerance extends EqualBy with Product {

    @transient override protected lazy val _equalBy: (String, List[Any]) =
      productPrefix -> productIterator.map(truncateToTolerance).toList

    def truncateToTolerance(v: Any): Any
  }

  trait Fields extends FieldsWithTolerance {

    def truncateToTolerance(v: Any): Any = v
  }

  def rectifyArray(id: Any): Any = {
    val result = id match {
      case aa: Array[_] => aa.toList
      case _            => id
    }
    result
  }
}
