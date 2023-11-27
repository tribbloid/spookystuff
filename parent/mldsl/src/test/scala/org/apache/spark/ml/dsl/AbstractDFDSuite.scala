package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.testutils.{BaseSpec, TestHelper}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}
import org.scalatest.BeforeAndAfterAll

import scala.util.matching.Regex

/**
  * Created by peng on 18/04/16.
  */
abstract class AbstractDFDSuite extends BaseSpec with BeforeAndAfterAll {

  implicit class StringView(str: String) extends super._StringView(str) {

    def treeNodeShouldBe(
        groundTruth: String = null,
        sort: Boolean = false
    ): Unit = {
      val compactedGT = Option(groundTruth).map(compactGroundTruth).orNull
      this.shouldBe(compactedGT, sort = sort)
    }
  }

  def compaction: PathCompaction = Compactions.DoNotCompact
  lazy val compactionOpt: Some[PathCompaction] = Some(compaction)

  def compactGroundTruth(str: String): String = {

    val regex: Regex = "(?<=[\\[\\,])[\\w\\$]*(?=[\\]\\,])".r
    val matches = regex.findAllIn(str).toList
    val cols = matches.map(_.split('$').toSeq).toSet
    val lookup = compaction(cols)

    val replaced = regex.replaceAllIn(
      str,
      { m =>
        val original: String = m.matched
        val multiPart = original.split('$').toSeq
        lookup(multiPart).mkString("\\$")
      }
    )

    replaced
  }

  def getInputsOutputs(stages: Seq[PipelineStage]): Seq[(String, String, String)] = {
    val input_output = stages.map { v =>
      val className = v.getClass.getSimpleName
      val input: Array[String] = v match {
        case v: HasInputCol  => Array(v.getInputCol)
        case v: HasInputCols => v.getInputCols
        case _               => Array[String]()
      }

      val output = v match {
        case v: HasOutputCol => Array(v.getOutputCol)
        case _               => Array[String]()
      }

      (className, input.toSeq.mkString("|"), output.toSeq.mkString("|"))
    }
    input_output
  }

  override def afterAll(): Unit = {

    TestHelper.cleanTempDirs()
    super.afterAll()
  }
}

trait UsePruneDownPath {
  self: AbstractDFDSuite =>

  override def compaction: Compactions.PruneDownPath.type = Compactions.PruneDownPath
}

trait UsePruneDownPathKeepRoot {
  self: AbstractDFDSuite =>

  override def compaction: Compactions.PruneDownPathKeepRoot.type = Compactions.PruneDownPathKeepRoot
}
