package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.testutils.TestMixin
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}
import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

import scala.util.Try
import scala.util.matching.Regex

/**
  * Created by peng on 18/04/16.
  */
abstract class AbstractFlowSuite extends FunSuite with TestMixin {

  implicit class StringView(str: String) extends super.TestStringView(str){

    def treeNodeShouldBe(groundTruth: String = null): Unit = {
      val compactedGT = Option(groundTruth).map(compactGroundTruth).orNull
      Try {
        this.shouldBe(compactedGT)
      }
        .recover {
          case e@ (_: TestFailedException | _: AssertionError) =>
            val correctedGT = compactedGT
              .replaceAll("+- ", " ")
              .replaceAll(":- ", " ")
              .replaceAll(":  ", " ")
            //this is for Spark 1.5
            this.shouldBe(correctedGT)
        }
    }
  }

  def compaction: PathCompaction = Compactions.DoNotCompact
  lazy val compactionOpt = Some(compaction)

  def compactGroundTruth(str: String) = {

    val regex: Regex = "(?<=[\\[\\,])[\\w\\$]*(?=[\\]\\,])".r
    val matches = regex.findAllIn(str).toList
    val cols = matches.map(_.split('$').toSeq).toSet
    val lookup = compaction(cols)

    val replaced = regex.replaceAllIn(
      str,
      {
        m =>
          val original: String = m.matched
          val multiPart = original.split('$').toSeq
          lookup(multiPart).mkString("\\$")
      }
    )

    replaced
  }

  def getInputsOutputs(stages: Seq[PipelineStage]): Seq[(String, String, String)] = {
    val input_output = stages.map {
      v =>
        val className = v.getClass.getSimpleName
        val input: Array[String] = v match {
          case v: HasInputCol => Array(v.getInputCol)
          case v: HasInputCols => v.getInputCols
          case _ => Array[String]()
        }

        val output = v match {
          case v: HasOutputCol => Array(v.getOutputCol)
          case _ => Array[String]()
        }

        (className, input.toSeq.mkString("|"), output.toSeq.mkString("|"))
    }
    input_output
  }
}

trait UsePruneDownPath {
  self: AbstractFlowSuite =>

  override def compaction = Compactions.PruneDownPath
}

trait UsePruneDownPathKeepRoot {
  self: AbstractFlowSuite =>

  override def compaction = Compactions.PruneDownPathKeepRoot
}