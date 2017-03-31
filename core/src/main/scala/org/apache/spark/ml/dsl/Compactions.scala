package org.apache.spark.ml.dsl

/**
  * Created by peng on 28/04/16.
  */
object Compactions {

  object DoNotCompact extends PathCompaction {

    override def apply(v1: Set[Seq[String]]): Map[Seq[String], Seq[String]] = {
      Map(v1.map(v => v -> v).toSeq: _*)
    }
  }

  object PruneDownPath extends PathCompaction {

    override def apply(names: Set[Seq[String]]): Map[Seq[String], Seq[String]] = {

      val trie = TrieNode.build(
        names
          .map(_.reverse)
          .map(v => v -> v)
      )

      val pairs = trie.pruneUp.flatMap{
        node =>
          val k = node.key
          node.value.map(_ -> k)
      }
        .map(tuple => tuple._1.reverse -> tuple._2.reverse)
      val lookup: Map[Seq[String], Seq[String]] = Map(pairs: _*)
      lookup
    }
  }

  object PruneDownPathKeepRoot extends PathCompaction {

    override def apply(names: Set[Seq[String]]): Map[Seq[String], Seq[String]] = {

      val trie = TrieNode.build(
        names
          .map(_.reverse)
          .map(v => v -> v)
      )

      val pairs = trie.pruneUp.flatMap{
        node =>
          val k = node.key
          node.value.map(_ -> k)
      }
        .map(
          tuple =>
            if (!tuple._2.endsWith(tuple._1.lastOption.toSeq)) {
              tuple._1 -> (tuple._2 ++ tuple._1.lastOption)
            }
            else {
              tuple._1 -> tuple._2
            }
        )
        .map(tuple => tuple._1.reverse -> tuple._2.reverse)
      val lookup: Map[Seq[String], Seq[String]] = Map(pairs: _*)
      lookup
    }
  }
}
