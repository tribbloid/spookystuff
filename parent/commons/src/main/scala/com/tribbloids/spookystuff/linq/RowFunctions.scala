package com.tribbloids.spookystuff.linq

import com.tribbloids.spookystuff.linq.Linq.{:=, Row, named}
import com.tribbloids.spookystuff.linq.LinqBase.Entry
import com.tribbloids.spookystuff.linq.internal.RowInternal

/**
  * mimicking [[org.apache.spark.sql.functions]] ()
  */
object RowFunctions {

  def explode[K <: XStr, V](
      selection: Entry[T1[K := Seq[V]]]
  ): Seq[Row[T1[K := V]]] = {
    val unboxed = Entry.unbox(selection)

    val seq = unboxed._internal.head[Seq[V]]

    seq.map { v =>
      val kv: K := V = named[K] := v

      RowInternal.ofElement(kv)
    }
  }
}
