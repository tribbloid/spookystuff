package org.tribbloid.spookystuff.pages

/**
 * Created by peng on 27/07/15.
 */
class TestPageFromFile extends TestPageFromHttp {

  override val htmlUrl = this.getClass.getClassLoader.getResource("site/Wikipedia.html").getPath
  override val pngUrl = this.getClass.getClassLoader.getResource("site/logo11w.png").getPath
  override val pdfUrl = this.getClass.getClassLoader.getResource("site/Test.pdf").getPath
}