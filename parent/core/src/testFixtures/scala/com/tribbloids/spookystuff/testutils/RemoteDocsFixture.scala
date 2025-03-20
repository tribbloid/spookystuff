package com.tribbloids.spookystuff.testutils

/**
  * Created by peng on 17/05/16.
  */
trait RemoteDocsFixture extends Serializable {

  def HTML_URL: String = "http://tribbloid.github.io/spookystuff/test/Wikipedia.html"
  def JSON_URL: String = "http://tribbloid.github.io/spookystuff/test/tribbloid.json"
  // TODO: add this after fetch can semi-auto-detect content-type
  //  def jsonUrlIncorrectContentType = "https://raw.githubusercontent.com/tribbloid/spookystuff/master/core/src/test/resources/site/tribbloid.json"
  def PNG_URL: String = "http://tribbloid.github.io/spookystuff/test/logo11w.png"
  def PDF_URL: String = "http://tribbloid.github.io/spookystuff/test/Test.pdf"
  def XML_URL: String = "http://tribbloid.github.io/spookystuff/test/example.xml"
  def CSV_URL: String = "http://tribbloid.github.io/spookystuff/test/table.csv"

  def HTTP_IP_URL: String = "http://api.ipify.org/"
  def HTTPS_IP_URL: String = "https://api.ipify.org/"
//  def USERAGENT_URL = "https://www.whatismybrowser.com/detect/what-is-my-user-agent/"

  def DUMMY_URL: String = "ftp://www.dummy.co"
}

object RemoteDocsFixture extends RemoteDocsFixture
