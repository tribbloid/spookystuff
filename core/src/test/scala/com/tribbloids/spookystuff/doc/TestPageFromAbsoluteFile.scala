package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.dsl

/**
 * Created by peng on 13/08/15.
 */
class TestPageFromAbsoluteFile extends TestPageFromFile {

  override def htmlUrl = "file://" + super.htmlUrl
  override def jsonUrl = "file://" + super.jsonUrl
  override def pngUrl = "file://" + super.pngUrl
  override def pdfUrl = "file://" + super.pdfUrl
  override def xmlUrl = "file://" + super.xmlUrl
  override def csvUrl = "file://" + super.csvUrl
  override def dirUrl = "file://" + super.dirUrl
}
