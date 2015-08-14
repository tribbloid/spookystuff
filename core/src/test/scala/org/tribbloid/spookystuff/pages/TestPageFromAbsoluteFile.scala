package org.tribbloid.spookystuff.pages

/**
 * Created by peng on 13/08/15.
 */
class TestPageFromAbsoluteFile extends TestPageFromFile {

  override def htmlUrl = "file://" + super.htmlUrl
  override def jsonUrl = "file://" + super.jsonUrl
  override def pngUrl = "file://" + super.pngUrl
  override def pdfUrl = "file://" + super.pdfUrl
}
