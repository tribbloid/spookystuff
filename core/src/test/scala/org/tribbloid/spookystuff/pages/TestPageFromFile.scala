package org.tribbloid.spookystuff.pages

/**
 * Created by peng on 27/07/15.
 */
class TestPageFromFile extends TestPageFromHttp {

  override def htmlUrl = this.getClass.getClassLoader.getResource("site/Wikipedia.html").getPath
  override def jsonUrl = this.getClass.getClassLoader.getResource("site/tribbloid.json").getPath
  override def pngUrl = this.getClass.getClassLoader.getResource("site/logo11w.png").getPath
  override def pdfUrl = this.getClass.getClassLoader.getResource("site/Test.pdf").getPath
}

class TestPageFromAbsoluteFile extends TestPageFromFile {

  override def htmlUrl = "file://" + super.htmlUrl
  override def jsonUrl = "file://" + super.jsonUrl
  override def pngUrl = "file://" + super.pngUrl
  override def pdfUrl = "file://" + super.pdfUrl
}