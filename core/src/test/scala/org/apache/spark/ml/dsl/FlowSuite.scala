package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector => MLVector}
import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.functions._

object FlowSuite {

  val TOKEN: String = "token"
  val STEMMED: String = "stemmed"
  val TF: String = "tf"
  val IDF: String = "idf"
  val TF_ZIPPED: String = "tf_zipped"
  val IDF_ZIPPED: String = "idf_zipped"

  val stemming = new StopWordsRemover()
  val tf = new HashingTF()
  val tfing = new feature.HashingTF(tf.getNumFeatures)

  val zipping: UserDefinedFunction = udf {
    (v1s: Seq[String], v2: MLVector) =>
      v1s.map(v => v -> v2(tfing.indexOf(v)))
  }
}



class FlowSuite extends AbstractFlowSuite {

  import FlowComponent._
  import FlowSuite._

  val training = TestHelper.TestSQL.createDataFrame(Seq(
    (0L, "a b c d e spark", 1.0),
    (1L, "b d", 0.0),
    (2L, "spark f g h", 1.0),
    (3L, "hadoop mapreduce", 0.0),
    (3L, "hadoop mapreduce", 1.0),
    (4L, "b spark who", 1.0),
    (5L, "g d a y", 0.0),
    (6L, "spark fly", 1.0),
    (7L, "was mapreduce", 0.0),
    (8L, "e spark program", 1.0),
    (9L, "a e c l", 0.0),
    (10L, "spark compile", 1.0),
    (11L, "hadoop software", 0.0)
  )).toDF("id", "input", "label")

  test("Flow can build Pipeline") {
    val part1 = (Flow('input)
      >-> new Tokenizer() -> TOKEN
      >-> stemming -> STEMMED
      >-> tf -> TF
      >-> new IDF() -> FlowSuite.IDF
      >- STEMMED <>- TF >>> UDFTransformer(zipping) -> TF_ZIPPED)

    val flow = part1
      .from(STEMMED) <>- FlowSuite.IDF >>> UDFTransformer(zipping) -> IDF_ZIPPED

    println(flow.show(showID = false, compactionOpt = compactionOpt))

    val pipeline = flow.build()

    val stages = pipeline.getStages
    val input_output = getInputsOutputs(stages)
    input_output.mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(StopWordsRemover,token,stemmed)
        |(HashingTF,stemmed,tf)
        |(UDFTransformer,stemmed|tf,tf_zipped)
        |(IDF,tf,idf)
        |(UDFTransformer,stemmed|idf,idf_zipped)
      """.stripMargin
    )
  }

  test("Pipeline can be visualized as ASCII art") {
    val flow = (
      Flow('input)
        >-> new Tokenizer() -> TOKEN
        >-> stemming -> STEMMED
        >-> tf -> TF
        >-> new IDF() -> FlowSuite.IDF
        >- STEMMED <>- TF >>> UDFTransformer(zipping) -> TF_ZIPPED
      )
      .from(STEMMED) <>- FlowSuite.IDF >>> UDFTransformer(zipping) -> IDF_ZIPPED

    flow.show(showID = false, showInputs = false, asciiArt = true).shouldBe(
      """
        |                             ┌───────────────┐
        |                             │(TAIL>) [input]│
        |                             └───────┬───────┘
        |                                     │
        |                                     v
        |                            ┌────────────────┐
        |                            │ token > [token]│
        |                            └────────┬───────┘
        |                                     │
        |                                     v
        |                          ┌────────────────────┐
        |                          │ stemmed > [stemmed]│
        |                          └─────┬───────┬─┬────┘
        |                                │       │ │
        |                    ┌───────────┘       │ └─────────────────────┐
        |                    │                   └───────┐               │
        |                    v                           │               │
        |              ┌──────────┐                      │               │
        |              │ tf > [tf]│                      │               │
        |              └───┬───┬──┘                      │               │
        |                  │   │                         │               │
        |                  │   └─────────────┐           │               │
        |                  v                 │           │               │
        |           ┌────────────┐           │           │               │
        |           │ idf > [idf]│           │           │               │
        |           └───┬────────┘           │           │               │
        |               │                    │           │               │
        |               │                    └───────────┼─────┐         │
        |               │            ┌───────────────────┘     │         │
        |               │            │                         │         │
        |               v            v                         v         v
        | ┌───────────────────────────────────────┐ ┌──────────────────────────────┐
        | │(HEAD)(<TAIL) idf_zipped > [idf_zipped]│ │(HEAD) tf_zipped > [tf_zipped]│
        | └───────────────────────────────────────┘ └──────────────────────────────┘
      """.stripMargin
    )
  }

  test("Pipeline can be visualized as ASCII art backwards") {
    val flow = (
      Flow('input)
        >-> new Tokenizer() -> TOKEN
        >-> stemming -> STEMMED
        >-> tf -> TF
        >-> new IDF() -> FlowSuite.IDF
        >- STEMMED <>- TF >>> UDFTransformer(zipping) -> TF_ZIPPED
      )
      .from(STEMMED) <>- FlowSuite.IDF >>> UDFTransformer(zipping) -> IDF_ZIPPED

    flow.show(showID = false, forward = false, asciiArt = true).shouldBe(
      """
        | ┌───────────────────────────────────────────────────────┐ ┌─────────────────────────────────────────────┐
        | │(HEAD)(<TAIL) [stemmed,idf] > idf_zipped > [idf_zipped]│ │(HEAD) [stemmed,tf] > tf_zipped > [tf_zipped]│
        | └───────────────────────────────────────────────────────┘ └─────────────────────────────────────────────┘
        |                    ^                  ^                                   ^              ^
        |                    │                  │                                   │              │
        |                    │                  └───────────────────────────┐       │              │
        |                    │                              ┌───────────────┼───────┘              │
        |                    │                              │               │                      │
        |               ┌────┴──────────────┐               │               │                      │
        |               │ [tf] > idf > [idf]│               │               │                      │
        |               └───────────────────┘               │               │                      │
        |                         ^                         │               │                      │
        |                         │         ┌───────────────┘               │                      │
        |                         │         │                               │                      │
        |                   ┌─────┴─────────┴──────┐                        │                      │
        |                   │ [stemmed] > tf > [tf]│                        │                      │
        |                   └──────────────────────┘                        │                      │
        |                               ^                                   │                      │
        |                               │                     ┌─────────────┘                      │
        |                               └─────────────┐       │       ┌────────────────────────────┘
        |                                             │       │       │
        |                                     ┌───────┴───────┴───────┴──────┐
        |                                     │ [token] > stemmed > [stemmed]│
        |                                     └──────────────────────────────┘
        |                                                     ^
        |                                                     │
        |                                       ┌─────────────┴────────────┐
        |                                       │ [input] > token > [token]│
        |                                       └──────────────────────────┘
        |                                                     ^
        |                                                     │
        |                                            ┌────────┴──────┐
        |                                            │(TAIL>) [input]│
        |                                            └───────────────┘
      """.stripMargin
    )
  }

  test("Flow can build PipelineModel") {
    val model = (
      Flow('input)
        >-> new Tokenizer() -> TOKEN
        >-> stemming -> STEMMED
        >-> tf -> TF
        >- STEMMED <>- TF >>> UDFTransformer(zipping) -> TF_ZIPPED
      ).buildModel()

    val stages = model.stages
    val input_output = getInputsOutputs(stages)
    input_output.mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(StopWordsRemover,token,stemmed)
        |(HashingTF,stemmed,tf)
        |(UDFTransformer,stemmed|tf,tf_zipped)
      """.stripMargin
    )

    val transformed = model.transform(training)

    transformed.show(false)
  }


  val validPart = (
    Flow('input)
      >-> new Tokenizer() -> TOKEN
      >-> tf -> TF
    )

  val validPart2 = Flow('label) >>> new OneHotEncoder() -> "label_one_hot"
  val irrelevantPart = Flow('dummy) >>> new OneHotEncoder() -> "dummy_one_hot"
  val typeInconsistentPart = Flow('label) >>> new Tokenizer() -> "label_cannot_be_tokenized"

  test("If adaptation = IgnoreIrrelevant, Flow can build a full pipeline given a valid schema evidence") {

    val complete = ((validPart U validPart2) >>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptations.IgnoreIrrelevant
      )

    getInputsOutputs(complete.getStages).mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(OneHotEncoder,label,label_one_hot)
        |(VectorAssembler,tf|label_one_hot,VectorAssembler)
      """.stripMargin
    )
  }

  test("If adaptation = IgnoreIrrelevant, Flow can build an incomplete pipeline when some of the sources are missing") {

    val incomplete = ((validPart U irrelevantPart) >>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptations.IgnoreIrrelevant
      )

    getInputsOutputs(incomplete.getStages).mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
      """.stripMargin
    )
  }

  test("If adaptation = IgnoreIrrelevant, Flow can build an incomplete pipeline when some of the sources have inconsistent type") {

    val incomplete = ((validPart U typeInconsistentPart) >>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptations.IgnoreIrrelevant
      )

    getInputsOutputs(incomplete.getStages).mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
      """.stripMargin
    )
  }

  test("If adaptation = IgnoreIrrelevant_TypeUnsafe, Flow can still build a full pipeline when some of the sources have inconsistent type") {

    val incomplete = ((validPart U typeInconsistentPart) >>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptations.IgnoreIrrelevant_TypeUnsafe
      )

    getInputsOutputs(incomplete.getStages).mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(Tokenizer,label,label_cannot_be_tokenized)
        |(VectorAssembler,tf|label_cannot_be_tokenized,VectorAssembler)
      """.stripMargin
    )
  }

  test("If adaptation = Force, Flow can still build a full pipeline when some of the sources are missing") {

    val forced = ((validPart U irrelevantPart) >>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptations.Force
      )

    getInputsOutputs(forced.getStages).mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(OneHotEncoder,dummy,dummy_one_hot)
        |(VectorAssembler,tf|dummy_one_hot,VectorAssembler)
      """.stripMargin
    )
  }

  test("If adaptation = Force, Flow can still build a full pipeline when some of the sources have inconsistent type") {

    val forced = ((validPart U typeInconsistentPart) >>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptations.Force
      )

    getInputsOutputs(forced.getStages).mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(Tokenizer,label,label_cannot_be_tokenized)
        |(VectorAssembler,tf|label_cannot_be_tokenized,VectorAssembler)
      """.stripMargin
    )
  }

  test("If adaption = FailFast, throw an exception when some of the sources are missing") {

    intercept[IllegalArgumentException](
      ((validPart U irrelevantPart) >>> new VectorAssembler())
        .build(
          dfEvidence = training,
          adaptation = SchemaAdaptations.FailFast
        )
    )
  }

  test("If adaption = FailFast, throw an exception when some of the sources have inconsistent type") {

    intercept[IllegalArgumentException](
      ((validPart U typeInconsistentPart) >>> new VectorAssembler())
        .build(
          dfEvidence = training,
          adaptation = SchemaAdaptations.FailFast
        )
    )
  }

  test("If adaptation = FailFast_TypeUnsafe, Flow can still build a full pipeline when some of the sources have inconsistent type") {

    val incomplete = ((validPart U typeInconsistentPart) >>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptations.FailFast_TypeUnsafe
      )

    getInputsOutputs(incomplete.getStages).mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(Tokenizer,label,label_cannot_be_tokenized)
        |(VectorAssembler,tf|label_cannot_be_tokenized,VectorAssembler)
      """.stripMargin
    )
  }

  test("If adaption = IgnoreIrrelevant_ValidateSchema, Flow can build an incomplete pipeline when some of the sources are missing") {

    val incomplete = ((validPart U irrelevantPart) >>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptations.IgnoreIrrelevant_ValidateSchema
      )

    getInputsOutputs(incomplete.getStages).mkString("\n").shouldBe(
      """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
      """.stripMargin
    )
  }

  test("If adaption = IgnoreIrrelevant_ValidateSchema, throw an exception when some of the sources have inconsistent type") {

    intercept[IllegalArgumentException](
      ((validPart U typeInconsistentPart) >>> new VectorAssembler())
        .build(
          dfEvidence = training,
          adaptation = SchemaAdaptations.IgnoreIrrelevant_ValidateSchema
        )
    )
  }
}