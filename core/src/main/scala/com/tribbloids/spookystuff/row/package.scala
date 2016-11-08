package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions.TraceView
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

package object row {

  type Data = Map[Field, Any]

  implicit val Data = Map

//  type FetchedRow = (DataRow, Seq[Fetched]) //TODO: change to SQL Row

  type SquashedFetchedRDD = RDD[SquashedFetchedRow]

  type Sampler[T] = Iterable[(T, Int)] => Iterable[(T, Int)] //with index

  type RowReducer = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]

  type RowOrdering = Ordering[(TraceView, Iterable[DataRow])]

  // f(open, visited) => open
  type RowEliminator = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]

  // In every execution plan, the schema: Map(Field -> DataType) has to be created on construction, which enables every Field to be cast into TypedField or IndexedField
  type IndexedField = (TypedField, Int)

  type SchemaRow = GenericRowWithSchema
}