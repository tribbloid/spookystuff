package org.tribbloid.spookystuff.pages

/**
 * Created by peng on 11/27/14.
 */
trait Unstructured extends Serializable {

   def uri: String

   def children(selector: String): Seq[Unstructured]

//   final def apply(selector: String) = children(selector)

   def markup: Option[String]

   def attr(attr: String, noEmpty: Boolean = true): Option[String]

   final def href = attr("abs:href")

   final def src = attr("abs:src")

   def text: Option[String]

   def ownText: Option[String]

   def boilerPipe(): Option[String]
}

final class UnstructuredSeqView(self: Seq[Unstructured]) {

   def uris: Seq[String] = self.map(_.uri)

   def allChildren(selector: String): Seq[Unstructured] = self.flatMap(_.children(selector))

   def markups: Seq[String] = self.flatMap(_.markup)

   def attrs(attr: String, noEmpty: Boolean = true): Seq[String] = self.flatMap(_.attr(attr, noEmpty))

   def hrefs(abs: Boolean) = attrs("abs:href")

   def srcs = attrs("abs:src")

   def texts: Seq[String] = self.flatMap(_.text)

   def ownTexts: Seq[String] = self.flatMap(_.ownText)

   def boilerPipes(): Seq[String] = self.flatMap(_.boilerPipe())
}

