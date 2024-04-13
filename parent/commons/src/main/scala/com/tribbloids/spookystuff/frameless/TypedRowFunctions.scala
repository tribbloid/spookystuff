package com.tribbloids.spookystuff.frameless

/**
  * mimicking [[org.apache.spark.sql.functions]] ()
  */
object TypedRowFunctions {

  def explode[K <: XStr, V](
      selection: TypedRow.ElementAPI[T1[K := Seq[V]]]
  ): Seq[TypedRow[T1[K := V]]] = {
    val unboxed = TypedRow.ElementAPI.unbox(selection)

    val seq = unboxed._internal.head[Seq[V]]

    seq.map { v =>
      val kv: K := V = named[K] := v

      TypedRowInternal.ofElement(kv)
    }
  }
}
