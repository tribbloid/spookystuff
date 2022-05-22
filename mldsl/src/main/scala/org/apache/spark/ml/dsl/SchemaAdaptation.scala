package org.apache.spark.ml.dsl

/**
  * Created by peng on 29/04/16.
  */
sealed abstract class SchemaAdaptation

object SchemaAdaptation {

  //disable schema validations ( e.g. Transformer.transformSchema)
  sealed trait TypeUnsafe extends SchemaAdaptation

  sealed trait FailOnInconsistentSchema extends SchemaAdaptation
  sealed trait FailOnNonExistingInputCol extends SchemaAdaptation

  object FailFast extends FailOnInconsistentSchema with FailOnNonExistingInputCol
  object FailFast_TypeUnsafe extends FailOnNonExistingInputCol with TypeUnsafe

  //allow incomplete output
  sealed abstract class IgnoreIrrelevant extends SchemaAdaptation

  object IgnoreIrrelevant extends IgnoreIrrelevant
  object IgnoreIrrelevant_TypeUnsafe extends IgnoreIrrelevant with TypeUnsafe

  object IgnoreIrrelevant_ValidateSchema extends IgnoreIrrelevant with FailOnInconsistentSchema

  object Force extends TypeUnsafe
}
