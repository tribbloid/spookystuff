package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.DocFilterImpl
import com.tribbloids.spookystuff.row.Field
import org.apache.tika.detect.DefaultDetector

import scala.language.implicitConversions

object Const {

  implicit def asCommonConst(v: this.type): CommonConst.type = CommonConst

  val defaultInputKey: String = "_"
  val keyDelimiter: String = "'"
  val onlyPageExtractor: String = "S"
  val allPagesExtractor: String = "S_*"

  val groupIndexExtractor: String = "G"

  val defaultForkField: Field = Field("A", isTransient = true)

  val tikaDetector: DefaultDetector = new DefaultDetector()

  val defaultDocumentFilter: DocFilterImpl.MustHaveTitle.type = DocFilterImpl.MustHaveTitle
  val defaultImageFilter: DocFilterImpl.AcceptStatusCode2XX.type = DocFilterImpl.AcceptStatusCode2XX

  val exploreStageSize: Int = 100
}
