//package org.apache.spark.ml.dsl
//
//import org.apache.spark.ml.dsl.utils.Xml
//import org.apache.spark.ml.feature._
//import org.apache.spark.mllib.linalg.{Vector => MLVector}
//import org.json4s.JValue
//
///**
//  * Created by peng on 06/10/16.
//  */
//class FlowReadWriteSuite extends AbstractFlowSuite {
//
//  import FlowComponent._
//  import FlowSuite._
//
//  val pipelinePath = "temp/pipeline/pipeline"
//
//  test("Pipeline can be saved and loaded") {
//    val flow = (Flow('input)
//      >-> new Tokenizer() -> TOKEN
//      >-> stemming -> STEMMED
//      >-> tf -> TF
//      >-> new IDF() -> FlowSuite.IDF
//      >- STEMMED <>- TF >>> UDFTransformer(zipping) -> TF_ZIPPED)
//      .from(STEMMED) <>- FlowSuite.IDF >>> UDFTransformer(zipping) -> IDF_ZIPPED
//
//    val pipeline = flow.build()
//
//    pipeline.write.overwrite().save(pipelinePath)
//    val pipeline2 = Pipeline.read.load(pipelinePath)
//
//    pipeline.toString().shouldBe(pipeline2.toString())
//  }
//
//
//  test("PipelineModel can be saved and loaded") {
//    val model = (
//      Flow('input)
//        >-> new Tokenizer() -> TOKEN
//        >-> stemming -> STEMMED
//        >-> tf -> TF
//        >- STEMMED <>- TF >>> UDFTransformer(zipping) -> TF_ZIPPED
//      ).buildModel()
//
//    model.write.overwrite().save(pipelinePath)
//    val model2 = PipelineModel.read.load(pipelinePath)
//
//    model.toString().shouldBe(model2.toString())
//  }
//
//
//  test("Flow can be serialized into JSON and back") {
//
//    val flow = (
//      Flow('input.string)
//        >-> new Tokenizer() -> 'token
//        >-> stemming -> 'stemmed
//        >-> tf -> 'tf
//        >-> new IDF() -> 'idf
//        >- "stemmed" <>- "tf" >>> UDFTransformer(zipping) -> 'tf_zipped
//      )
//      .from(STEMMED) <>- FlowSuite.IDF >>> UDFTransformer(zipping) -> 'idf_zipped
//
//    val prettyJSON = flow.write.message.prettyJSON
//
//    prettyJSON.shouldBe()
//
//    val flow2 = FlowRelay.fromJSON(prettyJSON).toObject
//
//    println(flow2.show(asciiArt = true))
//
//    flow.show().shouldBe(
//      flow2.show()
//    )
//  }
//
//  test("Flow can be serialized into XML and back") {
//
//    val flow = (
//      Flow('input.string)
//        >-> new Tokenizer() -> 'token
//        >-> stemming -> 'stemmed
//        >-> tf -> 'tf
//        >- "stemmed" <>- "tf" >>> UDFTransformer(zipping) -> 'tf_zipped
//      )
//
//    val jValue: JValue = "root" -> flow.write.message.toJValue
//    val jValue2 = Xml.toJson(Xml.toXml(jValue))
//
//    //    pretty(jValue).shouldBe(pretty(jValue2))
//
//    val prettyXML = flow.write.message.prettyXML
//
//    prettyXML.shouldBe()
//
//    val flow2 = FlowRelay.fromXML(prettyXML).toObject
//
//    println(flow2.show(asciiArt = true))
//
//    flow.show().shouldBe(
//      flow2.show()
//    )
//  }
//}
