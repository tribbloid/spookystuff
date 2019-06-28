package org.apache.spark.ml.dsl.utils.data

import scala.language.implicitConversions

object Magnets {

  case class KV[+T](
      k: String,
      vOpt: Option[T]
  )

  object KV {

    implicit def fromTuple1[T](kv: (String, T)): KV[T] = KV(kv._1, Some(kv._2))
    implicit def fromTuple2[T](kv: (AttrLike[T], T)): KV[T] = KV(kv._1.primaryName, Some(kv._2))

    implicit def fromItr[T, Src](kvs: Iterable[Src])(implicit ev: Src => KV[T]): Iterable[KV[T]] = kvs.map(ev)
  }

  case class K(names: Seq[String])

  object K {

    implicit def fromStr(v: String): K = K(Seq(v))
    implicit def fromAttr(v: AttrLike[_]): K = K(v.allNames)

    implicit def fromItr[Src](kss: Iterable[Src])(implicit ev: Src => K): Iterable[K] = kss.map(ev)
  }
}
