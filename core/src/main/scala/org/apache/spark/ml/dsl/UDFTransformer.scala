package org.apache.spark.ml.dsl

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{DefaultParamsReadable, DefaultParamsWritable, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.UserDefinedFunction

object UDFTransformer extends DefaultParamsReadable[UDFTransformer] {

  def apply(udf: UserDefinedFunction) = new UDFTransformer().setUDF(udf)

  override def load(path: String): UDFTransformer = super.load(path)
}

/**
  * Created by peng on 09/04/16.
  * TODO: use UDF registry's name as uid & name
  */
case class UDFTransformer(
                           uid: String = Identifiable.randomUID("udf")
                         )
  extends Transformer
    with HasInputCols
    with HasOutputCol
    with DefaultParamsWritable
    with DynamicParamsMixin {

  lazy val UDF = GenericParam[UserDefinedFunction]()

  import org.apache.spark.sql.functions._

  override def transform(dataset: DataFrame): DataFrame = {
    dataset.withColumn(outputCol,
      UDF(
        (inputCols: Array[String])
          .map(v => col(v)): _*)
    )
  }

  override def copy(extra: ParamMap): Transformer = this.defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol, UDF.dataType, nullable = true))
  }
}
