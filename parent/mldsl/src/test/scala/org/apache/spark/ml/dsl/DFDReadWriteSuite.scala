package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.json4s.JsonAST.JObject

/**
  * Created by peng on 06/10/16.
  */
class DFDReadWriteSuite extends AbstractDFDSuite {

  import DFDComponent._
  import org.apache.spark.ml.dsl.DFDSuite._
  import org.apache.spark.ml.dsl.utils.messaging.io.MLReadWriteSupports._

  TestHelper.TestSC

  val pipelinePath = "temp/pipeline/pipeline"
//  def sc: SparkContext = TestHelper.TestSpark

  it("Pipeline can be saved and loaded") {
    val flow = (DFD('input)
      :-> new Tokenizer() -> TOKEN
      :-> stemming -> STEMMED
      :-> tf -> TF
      :-> new IDF() -> IDF
      :>- STEMMED :&& TF :>> UDFTransformer(zipping) -> TF_ZIPPED)
      .from(STEMMED) :&& IDF :>> UDFTransformer(zipping) -> IDF_ZIPPED

    val pipeline: Pipeline = flow.build()

    pipeline.write.overwrite().save(pipelinePath)
    val pipeline2 = Pipeline.read.load(pipelinePath)

    pipeline.toString().shouldBe(pipeline2.toString())
  }

  it("PipelineModel can be saved and loaded") {
    val model = (
      DFD('input)
        :-> new Tokenizer() -> TOKEN
        :-> stemming -> STEMMED
        :-> tf -> TF
        :>- STEMMED :&& TF :>> UDFTransformer(zipping) -> TF_ZIPPED
    ).buildModel()

    model.write.overwrite().save(pipelinePath)
    val model2 = PipelineModel.read.load(pipelinePath)

    model.toString().shouldBe(model2.toString())
  }

  it("Flow can be serialized into JSON and back") {

    val flow = DFD('input.string) :->
      new Tokenizer() -> 'token :->
      stemming -> 'stemmed :->
      tf -> 'tf :->
      new IDF() -> 'idf

    val prettyJSON = flow.write.message.prettyJSON

//    prettyJSON.shouldBe()

    val flow2 = DFD.fromJSON(prettyJSON)

    flow
      .show()
      .shouldBe(
        flow2.show()
      )
  }

  it("Flow can be serialized into XML and back") {

    val flow = DFD('input.string) :->
      new Tokenizer() -> 'token :->
      stemming -> 'stemmed :->
      tf -> 'tf :->
      new IDF() -> 'idf

    JObject("root" -> flow.write.message.toJValue)
//    val jValue2 = Xml.toJson(Xml.toXml(jValue))

    //    pretty(jValue).shouldBe(pretty(jValue2))

    val prettyXML = flow.write.message.prettyXML

//    prettyXML.shouldBe()

    val flow2 = DFD.fromXML(prettyXML)

    flow
      .show()
      .shouldBe(
        flow2.show()
      )
  }
}
