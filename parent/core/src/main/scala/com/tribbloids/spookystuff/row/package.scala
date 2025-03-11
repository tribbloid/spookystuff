package com.tribbloids.spookystuff

import org.apache.spark.rdd.RDD

package object row {

  type SquashedRDD[D] = RDD[SquashedRow[D]]

  object SquashedRDD {

    type WithSchema[D] = RDD[SquashedRow[D]#_WithSchema]
  }

  type BeaconRDD[K] = RDD[(K, Unit)]

//  type Sampler[T] = Iterable[(T, Int)] => Iterable[(T, Int)] // with index

  // f(open, visited) => open
//  type RowEliminator = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]

  // In every execution plan, the schema: Map(Field -> DataType) has to be created on construction, which enables every Field to be cast into TypedField or IndexedField
//  type IndexedField = (TypedField, Int)

//  type SchemaRow = GenericRowWithSchema
}
