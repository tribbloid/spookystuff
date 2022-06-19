package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.utils.TreeThrowable

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * the ClassTag is an enforcement on all recursive members of v
  */
//TODO: this API should be merged into NestedMap
case class Nested[T: ClassTag](
    self: Any,
    errorTreeOpt: Option[Throwable] = None
) {

  def map[S: ClassTag](
      fn: T => S,
      fallback: Any => S = { _: Any =>
        null.asInstanceOf[S]
      },
      preproc: Any => Any = identity,
      failFast: Boolean = true
  ): Nested[S] = {

    val errorBuffer = ArrayBuffer.empty[Throwable]

    def mapper(v: Any): Any = {
      val result: Nested[S] = Nested[T](v).map(fn, fallback, preproc, failFast)
      result.errorTreeOpt.foreach(errorBuffer += _)
      result.self
    }

    //    def expandProduct(prod: Product): Map[String, Any] = {
    //      val map = Map(ReflectionUtils.getCaseAccessorMap(prod): _*)
    //      map.mapValues(mapper)
    //    }

    val mapped = preproc(self) match {
      case map: Map[_, _] =>
        map.mapValues(mapper)
      case array: Array[_] =>
        array.toSeq.map(mapper)
      case seq: Traversable[_] =>
        seq.map(mapper)
      //      case (k: Any, v: Any) =>
      //        k -> mapper(v)
      case v: T =>
        try {
          fn(v)
        } catch {
          case e: Exception =>
            //            v match {
            //              case product: Product =>
            //                expandProduct(product)
            //              case _ =>
            errorBuffer += e
            fallback(v)
          //            }
        }
      //      case prod: Product =>
      //        expandProduct(prod)
      case null =>
        null
      case v @ _ =>
        val e = new UnsupportedOperationException(s"$v is not an instance of ${implicitly[ClassTag[T]]}")
        errorBuffer += e
        fallback(v)
    }

    val treeException = if (errorBuffer.nonEmpty) {

      val treeException = TreeThrowable.combine(errorBuffer.toSeq)
      if (failFast) throw treeException
      Some(treeException)
    } else {
      None
    }
    Nested[S](mapped, treeException)
  }
}
