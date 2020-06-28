package org.apache.spark.ml.dsl

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StructField, StructType}

abstract class UDFTransformerLike extends Transformer with HasOutputCol with DynamicParamsMixin {

  def udfImpl: UserDefinedFunction

  def setUDFSafely(_udfImpl: UserDefinedFunction): UDFTransformerLike.this.type = {
    _udfImpl.inputTypes.toSeq.flatten.foreach { dataType =>
      assert(!dataType.isInstanceOf[VectorUDT], s"UDF input type ${classOf[VectorUDT].getCanonicalName} is obsolete!")
    }
    assert(!_udfImpl.dataType.isInstanceOf[VectorUDT],
           s"UDF output type ${classOf[VectorUDT].getCanonicalName} is obsolete!")
    this.setUDF(_udfImpl)
  }

  def getInputCols: Array[String]

  import org.apache.spark.sql.functions._

  override def transform(dataset: Dataset[_]): DataFrame = {
    val newCol = udfImpl(
      (getInputCols: Array[String])
        .map(v => col(v)): _*)

    val result = dataset.withColumn(outputCol, newCol)
    result
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(getOutputCol, udfImpl.dataType, nullable = true))
  }
}

object UDFTransformer extends DefaultParamsReadable[UDFTransformer] {

  def apply(udf: UserDefinedFunction): UDFTransformer = new UDFTransformer().setUDFSafely(udf)

  override def load(path: String): UDFTransformer = super.load(path)
}

/**
  * Created by peng on 09/04/16.
  * TODO: use UDF registry's name as uid & name
  */
case class UDFTransformer(
    uid: String = Identifiable.randomUID("udf")
) extends UDFTransformerLike
    with HasInputCols
    with DefaultParamsWritable {

  lazy val UDF: Param[UserDefinedFunction] = GenericParam[UserDefinedFunction]()
  def udfImpl: UserDefinedFunction = UDF

  override def copy(extra: ParamMap): Transformer = this.defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol, UDF.dataType, nullable = true))
  }

}
