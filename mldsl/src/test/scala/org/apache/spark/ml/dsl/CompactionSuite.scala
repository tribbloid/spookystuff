package org.apache.spark.ml.dsl

/**
  * Created by peng on 27/04/16.
  */
class CompactionSuite extends AbstractFlowSuite {

  val s1: Array[Seq[String]] =
    """
      |AB
      |ABC
      |ABCD
      |ABCDE
      |ABK
      |ABCK
      |ABCDK
      |ABCDEK
    """.trim.stripMargin
      .split("\n")
      .map(v => v.split("").toSeq)

  val s2: Array[Seq[String]] =
    """
      |input Tokenizer1
      |input Tokenizer0
      |input Tokenizer HashingTF
      |input Tokenizer StopWordsRemover HashingTF
      |input Tokenizer HashingTF VectorAssembler
      |input Tokenizer StopWordsRemover HashingTF VectorAssembler
      |input
      |input Tokenizer StopWordsRemover
    """.trim.stripMargin
      .split("\n")
      .map(v => v.split(" ").toSeq)

  def testCompaction(compaction: PathCompaction, source: Array[Seq[String]], expected: String = null): Unit = {

    assert(source.distinct.length == source.length)

    val lookup = compaction(source.toSet)
    val compare = source.map { v =>
      v -> lookup(v)
    }
    compare
      .mkString("\n")
      .shouldBe(
        )
    val compactedValues = compare.map(_._2).toList
    assert(compactedValues.distinct.size == compactedValues.size)
  }

  Seq(
    Compactions.DoNotCompact,
    Compactions.PruneDownPath,
    Compactions.PruneDownPathKeepRoot
  ).foreach { v =>
    Seq(s1, s2).foreach { s =>
      it(s"${v.getClass.getSimpleName.stripSuffix("$")} should work on ${s.head} ...") {

        testCompaction(v, s, null)
      }
    }

  }
}
