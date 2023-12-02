package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions.Trace
import org.apache.spark.rdd.RDD

package object row {

  type Data = Map[Field, Any] // TODO: change to SQL Row

  implicit val Data: Map.type = Map

  type BottleneckRDD = RDD[BottleneckRow]

  type BeaconRDD[K] = RDD[(K, Unit)]

  type Sampler[T] = Iterable[(T, Int)] => Iterable[(T, Int)] // with index

  type RowOrdering = Ordering[(Trace, Vector[DataRow])]

  // f(open, visited) => open
//  type RowEliminator = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]

  // In every execution plan, the schema: Map(Field -> DataType) has to be created on construction, which enables every Field to be cast into TypedField or IndexedField
  type IndexedField = (TypedField, Int)

//  type SchemaRow = GenericRowWithSchema
}
