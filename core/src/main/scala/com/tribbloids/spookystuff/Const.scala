package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.DocFilterImpl
import com.tribbloids.spookystuff.row.Field
import com.tribbloids.spookystuff.utils.CommonConst
import org.apache.tika.detect.DefaultDetector

object Const extends CommonConst {

  val defaultInputKey = "_"
  val keyDelimiter = "'"
  val onlyPageExtractor = "S"
  val allPagesExtractor = "S_*"

  val groupIndexExtractor = "G"

  val defaultJoinField = Field("A", isWeak = true)

  val mimeDetector = new DefaultDetector()

  val defaultDocumentFilter = DocFilterImpl.MustHaveTitle
  val defaultImageFilter = DocFilterImpl.AcceptStatusCode2XX

  val exploreStageSize = 100
}
