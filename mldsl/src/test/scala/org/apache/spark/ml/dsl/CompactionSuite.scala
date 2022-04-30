package org.apache.spark.ml.dsl

/**
  * Created by peng on 27/04/16.
  */
class CompactionSuite extends AbstractDFDSuite {

  trait TestCase extends Product {

    def original: String

    def compact1: String

    def compact2: String

    val s1: Array[Seq[String]] = original.trim.stripMargin
      .split("\n")
      .map(v => v.split(" ").toSeq)
  }

  case object Case1 extends TestCase {

    override def original: String =
      """
        |A B
        |A B C
        |A B C D
        |A B C D E
        |A B K
        |A B C K
        |A B C D K
        |A B C D E K
        |""".stripMargin

    override def compact1: String =
      """
        |B
        |C
        |D
        |E
        |B K
        |C K
        |D K
        |E K
        |""".stripMargin

    override def compact2: String =
      """
        |A B
        |A C
        |A D
        |A E
        |A B K
        |A C K
        |A D K
        |A E K
        |""".stripMargin
  }

  case object Case2 extends TestCase {
    override def original: String =
      """
        |input Tokenizer1
        |input Tokenizer0
        |input Tokenizer HashingTF
        |input Tokenizer StopWordsRemover HashingTF
        |input Tokenizer HashingTF VectorAssembler
        |input Tokenizer StopWordsRemover HashingTF VectorAssembler
        |input
        |input Tokenizer StopWordsRemover
        |""".stripMargin

    override def compact1: String =
      """
        |Tokenizer1
        |Tokenizer0
        |Tokenizer HashingTF
        |StopWordsRemover HashingTF
        |Tokenizer VectorAssembler
        |StopWordsRemover VectorAssembler
        |input
        |StopWordsRemover
        |""".stripMargin

    override def compact2: String =
      """
        |input Tokenizer1
        |input Tokenizer0
        |input Tokenizer HashingTF
        |input StopWordsRemover HashingTF
        |input Tokenizer VectorAssembler
        |input StopWordsRemover VectorAssembler
        |input
        |input StopWordsRemover
        |""".stripMargin
  }

  def testCompaction(compaction: PathCompaction, source: Array[Seq[String]], expected: String = null): Unit = {

    assert(source.distinct.length == source.length)

    val lookup = compaction(source.toSet)
    val compare = source.map { v =>
      v -> lookup(v)
    }
    compare
      .map { v =>
        v._2.mkString(" ")
      }
      .mkString("\n")
      .shouldBe(
        expected
      )
    val compactedValues = compare.map(_._2).toList
    assert(compactedValues.distinct.size == compactedValues.size)
  }

  Seq(
    Compactions.DoNotCompact -> { v: TestCase =>
      v.original
    },
    Compactions.PruneDownPath -> { v: TestCase =>
      v.compact1
    },
    Compactions.PruneDownPathKeepRoot -> { v: TestCase =>
      v.compact2
    }
  ).foreach {
    case (v, selector) =>
      Seq(Case1, Case2).foreach { c =>
        it(s"${v.getClass.getSimpleName.stripSuffix("$")} should work on ${c.toString}  ...") {

          testCompaction(v, c.s1, selector(c))
        }
      }

  }
}
