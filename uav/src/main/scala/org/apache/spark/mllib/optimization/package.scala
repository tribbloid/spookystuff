package org.apache.spark.mllib

package object optimization {

  type Vec = breeze.linalg.Vector[Double]
  def Vec = breeze.linalg.Vector

  type DVec = breeze.linalg.DenseVector[Double]
  type SVec = breeze.linalg.SparseVector[Double]

  type MLVec = org.apache.spark.mllib.linalg.Vector
  def MLVecs = org.apache.spark.mllib.linalg.Vectors

  type MLDVec = org.apache.spark.mllib.linalg.DenseVector
  type MLSVec = org.apache.spark.mllib.linalg.SparseVector
}
