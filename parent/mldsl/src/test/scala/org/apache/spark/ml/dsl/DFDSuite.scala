package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.feature
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object DFDSuite {

  val TOKEN = 'token
  val STEMMED: String = "stemmed"
  val TF: String = "tf"
  val IDF: String = "idf"
  val TF_ZIPPED: String = "tf_zipped"
  val IDF_ZIPPED: String = "idf_zipped"

  val stemming = new StopWordsRemover()
  val tf = new HashingTF()
  val tfing = new feature.HashingTF(tf.getNumFeatures)

  val zipping: UserDefinedFunction = udf { (v1s: Seq[String], v2: MLVector) =>
    v1s.map(v => v -> v2(tfing.indexOf(v)))
  }
}

class DFDSuite extends AbstractDFDSuite {

  import DFDComponent._
  import DFDSuite._

  override lazy val compactionOpt: Some[PathCompaction] = Some(Compactions.PruneDownPath)

  val training: DataFrame = TestHelper.TestSQL
    .createDataFrame(
      Seq(
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
      ))
    .toDF("id", "input", "label")

  it("Flow can build Pipeline") {
    val part1 = (
      DFD('input)
        :-> new Tokenizer() -> TOKEN
        :-> stemming -> STEMMED
        :-> tf -> TF
        :-> new IDF() -> DFDSuite.IDF
        :>- STEMMED :&& TF :>> UDFTransformer(zipping) -> TF_ZIPPED
    )

    val flow = part1
      .from(STEMMED) :&& DFDSuite.IDF :>> UDFTransformer(zipping) -> IDF_ZIPPED

    flow
      .show(showID = false, compactionOpt = compactionOpt, asciiArt = true)
      .shouldBe(
        """
          |                                            ┌───────────────┐
          |                                            │(TAIL>) [input]│
          |                                            └────────┬──────┘
          |                                                     │
          |                                                     v
          |                                       ┌──────────────────────────┐
          |                                       │ [input] > token > [token]│
          |                                       └─────────────┬────────────┘
          |                                                     │
          |                                                     v
          |                                     ┌──────────────────────────────┐
          |                                     │ [token] > stemmed > [stemmed]│
          |                                     └───────┬───────────────┬─────┬┘
          |                                             │               │     │
          |                               ┌─────────────┘               │     │
          |                               │                             │     │
          |                               v                             │     │
          |                   ┌──────────────────────┐                  │     │
          |                   │ [stemmed] > tf > [tf]│                  │     │
          |                   └─────┬─────────┬──────┘                  │     │
          |                         │         │                         │     │
          |                         │         │                         └─────┼───────────────┐
          |                         │         │   ┌───────────────────────────┘               │
          |                         │         └───┼───────────────────────────────────┐       │
          |                         v             │                                   │       │
          |               ┌───────────────────┐   │                                   │       │
          |               │ [tf] > idf > [idf]│   │                                   │       │
          |               └────┬──────────────┘   │                                   │       │
          |                    │                  │                                   │       │
          |                    v                  v                                   v       v
          | ┌───────────────────────────────────────────────────────┐ ┌─────────────────────────────────────────────┐
          | │(HEAD)(<TAIL) [stemmed,idf] > idf_zipped > [idf_zipped]│ │(HEAD) [stemmed,tf] > tf_zipped > [tf_zipped]│
          | └───────────────────────────────────────────────────────┘ └─────────────────────────────────────────────┘
          |""".stripMargin
      )

    val pipeline = flow.build()

    val stages = pipeline.getStages
    val input_output = getInputsOutputs(stages)
    input_output
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(StopWordsRemover,token,stemmed)
        |(HashingTF,stemmed,tf)
        |(UDFTransformer,stemmed|tf,tf_zipped)
        |(IDF,tf,idf)
        |(UDFTransformer,stemmed|idf,idf_zipped)
      """.stripMargin
      )

//    val outDF = pipeline.fit(training).transform(training)
//    outDF.show()
  }

  it("Pipeline can be visualized as ASCII art") {
    val flow = (
      DFD('input)
        :-> new Tokenizer() -> TOKEN
        :-> stemming -> STEMMED
        :-> tf -> TF
        :-> new IDF() -> DFDSuite.IDF
        :>- STEMMED :&& TF :>> UDFTransformer(zipping) -> TF_ZIPPED
    ).from(STEMMED) :&& DFDSuite.IDF :>> UDFTransformer(zipping) -> IDF_ZIPPED

    flow
      .show(showID = false, showInputs = false, asciiArt = true)
      .shouldBe(
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
        |                    ┌───────────┘       │ └─────────────────┐
        |                    │       ┌───────────┘                   │
        |                    v       │                               │
        |              ┌──────────┐  │                               │
        |              │ tf > [tf]│  │                               │
        |              └───┬───┬──┘  │                               │
        |                  │   │     │                               │
        |                  │   └─────┼─────────────────────────┐     │
        |                  v         │                         │     │
        |           ┌────────────┐   │                         │     │
        |           │ idf > [idf]│   │                         │     │
        |           └───┬────────┘   │                         │     │
        |               │            │                         │     │
        |               v            v                         v     v
        | ┌───────────────────────────────────────┐ ┌──────────────────────────────┐
        | │(HEAD)(<TAIL) idf_zipped > [idf_zipped]│ │(HEAD) tf_zipped > [tf_zipped]│
        | └───────────────────────────────────────┘ └──────────────────────────────┘
      """.stripMargin
      )
  }

  it("Pipeline can be visualized as ASCII art backwards") {
    val flow = (
      DFD('input)
        :-> new Tokenizer() -> TOKEN
        :-> stemming -> STEMMED
        :-> tf -> TF
        :-> new IDF() -> DFDSuite.IDF
        :>- STEMMED :&& TF :>> UDFTransformer(zipping) -> TF_ZIPPED
    ).from(STEMMED) :&& DFDSuite.IDF :>> UDFTransformer(zipping) -> IDF_ZIPPED

    flow
      .show(showID = false, forward = false, asciiArt = true)
      .shouldBe(
        """
        | ┌───────────────────────────────────────────────────────┐ ┌─────────────────────────────────────────────┐
        | │(HEAD)(<TAIL) [stemmed,idf] > idf_zipped > [idf_zipped]│ │(HEAD) [stemmed,tf] > tf_zipped > [tf_zipped]│
        | └───────────────────────────────────────────────────────┘ └─────────────────────────────────────────────┘
        |                    ^                  ^                                   ^       ^
        |                    │                  │                                   │       │
        |               ┌────┴──────────────┐   │                                   │       │
        |               │ [tf] > idf > [idf]│   │                                   │       │
        |               └───────────────────┘   │                                   │       │
        |                         ^             │                                   │       │
        |                         │         ┌───┼───────────────────────────────────┘       │
        |                         │         │   └───────────────────────────┐               │
        |                         │         │                         ┌─────┼───────────────┘
        |                         │         │                         │     │
        |                   ┌─────┴─────────┴──────┐                  │     │
        |                   │ [stemmed] > tf > [tf]│                  │     │
        |                   └──────────────────────┘                  │     │
        |                               ^                             │     │
        |                               │                             │     │
        |                               └─────────────┐               │     │
        |                                             │               │     │
        |                                     ┌───────┴───────────────┴─────┴┐
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

  it("Flow can build PipelineModel") {
    val model = (
      DFD('input)
        :-> new Tokenizer() -> TOKEN
        :-> stemming -> STEMMED
        :-> tf -> TF
        :>- STEMMED :&& TF :>> UDFTransformer(zipping) -> TF_ZIPPED
    ).buildModel()

    val stages = model.stages
    val input_output = getInputsOutputs(stages)
    input_output
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(StopWordsRemover,token,stemmed)
        |(HashingTF,stemmed,tf)
        |(UDFTransformer,stemmed|tf,tf_zipped)
      """.stripMargin
      )

    val transformed = model.transform(training)

    transformed
      .collect()
      .mkString("\n")
      .shouldBe(
        """
          |[0,a b c d e spark,1.0,WrappedArray(a, b, c, d, e, spark),WrappedArray(b, c, d, e, spark),(262144,[17222,27526,28698,30913,234657],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([b,1.0], [c,1.0], [d,1.0], [e,1.0], [spark,1.0])]
          |[1,b d,0.0,WrappedArray(b, d),WrappedArray(b, d),(262144,[27526,30913],[1.0,1.0]),WrappedArray([b,1.0], [d,1.0])]
          |[2,spark f g h,1.0,WrappedArray(spark, f, g, h),WrappedArray(spark, f, g, h),(262144,[15554,24152,51505,234657],[1.0,1.0,1.0,1.0]),WrappedArray([spark,1.0], [f,1.0], [g,1.0], [h,1.0])]
          |[3,hadoop mapreduce,0.0,WrappedArray(hadoop, mapreduce),WrappedArray(hadoop, mapreduce),(262144,[42633,155117],[1.0,1.0]),WrappedArray([hadoop,1.0], [mapreduce,1.0])]
          |[3,hadoop mapreduce,1.0,WrappedArray(hadoop, mapreduce),WrappedArray(hadoop, mapreduce),(262144,[42633,155117],[1.0,1.0]),WrappedArray([hadoop,1.0], [mapreduce,1.0])]
          |[4,b spark who,1.0,WrappedArray(b, spark, who),WrappedArray(b, spark),(262144,[30913,234657],[1.0,1.0]),WrappedArray([b,1.0], [spark,1.0])]
          |[5,g d a y,0.0,WrappedArray(g, d, a, y),WrappedArray(g, d, y),(262144,[27526,51505,130027],[1.0,1.0,1.0]),WrappedArray([g,1.0], [d,1.0], [y,1.0])]
          |[6,spark fly,1.0,WrappedArray(spark, fly),WrappedArray(spark, fly),(262144,[213423,234657],[1.0,1.0]),WrappedArray([spark,1.0], [fly,1.0])]
          |[7,was mapreduce,0.0,WrappedArray(was, mapreduce),WrappedArray(mapreduce),(262144,[42633],[1.0]),WrappedArray([mapreduce,1.0])]
          |[8,e spark program,1.0,WrappedArray(e, spark, program),WrappedArray(e, spark, program),(262144,[17222,210452,234657],[1.0,1.0,1.0]),WrappedArray([e,1.0], [spark,1.0], [program,1.0])]
          |[9,a e c l,0.0,WrappedArray(a, e, c, l),WrappedArray(e, c, l),(262144,[17222,28698,213302],[1.0,1.0,1.0]),WrappedArray([e,1.0], [c,1.0], [l,1.0])]
          |[10,spark compile,1.0,WrappedArray(spark, compile),WrappedArray(spark, compile),(262144,[89047,234657],[1.0,1.0]),WrappedArray([spark,1.0], [compile,1.0])]
          |[11,hadoop software,0.0,WrappedArray(hadoop, software),WrappedArray(hadoop, software),(262144,[123474,155117],[1.0,1.0]),WrappedArray([hadoop,1.0], [software,1.0])]
          |""".stripMargin
      )

  }

  val validPart: DFD = (
    DFD('input)
      :-> new Tokenizer() -> TOKEN
      :-> tf -> TF
  )

  val validPart2: DFD = DFD('label) :>> new OneHotEncoder() -> "label_one_hot"
  val irrelevantPart: DFD = DFD('dummy) :>> new OneHotEncoder() -> "dummy_one_hot"
  val typeInconsistentPart: DFD = DFD('label) :>> new Tokenizer() -> "label_cannot_be_tokenized"

  it("If adaptation = IgnoreIrrelevant, Flow can build a full pipeline given a valid schema evidence") {

    val complete = ((validPart U validPart2) :>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptation.IgnoreIrrelevant
      )

    getInputsOutputs(complete.getStages)
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(OneHotEncoder,label,label_one_hot)
        |(VectorAssembler,tf|label_one_hot,VectorAssembler)
      """.stripMargin
      )
  }

  it("If adaptation = IgnoreIrrelevant, Flow can build an incomplete pipeline when some of the sources are missing") {

    val incomplete = ((validPart U irrelevantPart) :>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptation.IgnoreIrrelevant
      )

    getInputsOutputs(incomplete.getStages)
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
      """.stripMargin
      )
  }

  it(
    "If adaptation = IgnoreIrrelevant, Flow can build an incomplete pipeline when some of the sources have inconsistent type") {

    val incomplete = ((validPart U typeInconsistentPart) :>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptation.IgnoreIrrelevant
      )

    getInputsOutputs(incomplete.getStages)
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
      """.stripMargin
      )
  }

  it(
    "If adaptation = IgnoreIrrelevant_TypeUnsafe, Flow can still build a full pipeline when some of the sources have inconsistent type") {

    val incomplete = ((validPart U typeInconsistentPart) :>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptation.IgnoreIrrelevant_TypeUnsafe
      )

    getInputsOutputs(incomplete.getStages)
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(Tokenizer,label,label_cannot_be_tokenized)
        |(VectorAssembler,tf|label_cannot_be_tokenized,VectorAssembler)
      """.stripMargin
      )
  }

  it("If adaptation = Force, Flow can still build a full pipeline when some of the sources are missing") {

    val forced = ((validPart U irrelevantPart) :>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptation.Force
      )

    getInputsOutputs(forced.getStages)
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(OneHotEncoder,dummy,dummy_one_hot)
        |(VectorAssembler,tf|dummy_one_hot,VectorAssembler)
      """.stripMargin
      )
  }

  it("If adaptation = Force, Flow can still build a full pipeline when some of the sources have inconsistent type") {

    val forced = ((validPart U typeInconsistentPart) :>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptation.Force
      )

    getInputsOutputs(forced.getStages)
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(Tokenizer,label,label_cannot_be_tokenized)
        |(VectorAssembler,tf|label_cannot_be_tokenized,VectorAssembler)
      """.stripMargin
      )
  }

  it("If adaption = FailFast, throw an exception when some of the sources are missing") {

    intercept[IllegalArgumentException](
      ((validPart U irrelevantPart) :>> new VectorAssembler())
        .build(
          dfEvidence = training,
          adaptation = SchemaAdaptation.FailFast
        )
    )
  }

  it("If adaption = FailFast, throw an exception when some of the sources have inconsistent type") {

    intercept[IllegalArgumentException](
      ((validPart U typeInconsistentPart) :>> new VectorAssembler())
        .build(
          dfEvidence = training,
          adaptation = SchemaAdaptation.FailFast
        )
    )
  }

  it(
    "If adaptation = FailFast_TypeUnsafe, Flow can still build a full pipeline when some of the sources have inconsistent type") {

    val incomplete = ((validPart U typeInconsistentPart) :>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptation.FailFast_TypeUnsafe
      )

    getInputsOutputs(incomplete.getStages)
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
        |(Tokenizer,label,label_cannot_be_tokenized)
        |(VectorAssembler,tf|label_cannot_be_tokenized,VectorAssembler)
      """.stripMargin
      )
  }

  it(
    "If adaption = IgnoreIrrelevant_ValidateSchema, Flow can build an incomplete pipeline when some of the sources are missing") {

    val incomplete = ((validPart U irrelevantPart) :>> new VectorAssembler())
      .build(
        dfEvidence = training,
        adaptation = SchemaAdaptation.IgnoreIrrelevant_ValidateSchema
      )

    getInputsOutputs(incomplete.getStages)
      .mkString("\n")
      .shouldBe(
        """
        |(Tokenizer,input,token)
        |(HashingTF,token,tf)
      """.stripMargin
      )
  }

  it(
    "If adaption = IgnoreIrrelevant_ValidateSchema, throw an exception when some of the sources have inconsistent type") {

    intercept[IllegalArgumentException](
      ((validPart U typeInconsistentPart) :>> new VectorAssembler())
        .build(
          dfEvidence = training,
          adaptation = SchemaAdaptation.IgnoreIrrelevant_ValidateSchema
        )
    )
  }
}