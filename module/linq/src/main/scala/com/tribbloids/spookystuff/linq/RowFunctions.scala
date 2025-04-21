package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.compat.NamedTupleX.:=
import ai.acyclic.prover.commons.compat.TupleX.T1
import ai.acyclic.prover.commons.compat.{Key, XStr}
import com.tribbloids.spookystuff.linq.Rec
import com.tribbloids.spookystuff.linq.Foundation.KVPairs

/**
  * mimicking [[org.apache.spark.sql.functions]] ()
  */
object RowFunctions {

  def explode[K <: XStr, V](
      selection: KVPairs[T1[K := Seq[V]]]
  ): Seq[Rec[T1[K := V]]] = {
    val unboxed = KVPairs.unbox(selection)

    val seq = unboxed._internal.head[Seq[V]]

    seq.map { v =>
      val kv: K := V = Key[K] := v

      Rec.ofTagged(kv)
    }
  }
}
