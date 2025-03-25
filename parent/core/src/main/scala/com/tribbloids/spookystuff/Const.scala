package com.tribbloids.spookystuff

import org.apache.tika.detect.DefaultDetector

import scala.language.implicitConversions

object Const {

  implicit def asCommonConst(v: this.type): CommonConst.type = CommonConst

  val defaultInputKey: String = "_"
  val keyDelimiter: String = "'"
  val onlyPageExtractor: String = "S"
  val allPagesExtractor: String = "S_*"

  val groupIndexExtractor: String = "G"

  val tikaDetector: DefaultDetector = new DefaultDetector()

  val exploreStageSize: Int = 100
}
