package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.UseFleet
import com.tribbloids.spookystuff.uav.dsl.ActionCosts
import com.tribbloids.spookystuff.uav.system.Drone
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.commons.math3.exception.MathIllegalArgumentException
import org.apache.commons.math3.exception.util.LocalizedFormats
import org.apache.commons.math3.genetics._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class SubSeq(
                   drone: Drone,
                   seqI: List[Int]
                 ) {

}

/**
  * Multi-Depot k-Rural Postman Problem solver
  * genetic algorithm comes in handy.
  * use 1 shuffling per generation.
  * Unfortunately this may be suboptimal comparing to http://niels.nu/blog/2016/spark-of-life-genetic-algorithm.html
  * which has many micro local generations per shuffling.
  * takes further testing to know if the convenience of local estimation worths more shuffling.
  */
case class MDkRPPSolver(
                         allTraces: List[Trace],
                         conf: UAVConf,
                         spooky: SpookyContext
                       ) {

  val broadcastedTraces = spooky.sparkContext.broadcast(allTraces)

  @transient val allHypotheses: ArrayBuffer[Hypothesis] = ArrayBuffer.empty

  def evaluateMissingFitness(): Unit = this.synchronized{
    val unevaluatedHypotheses = MDkRPPSolver.this.allHypotheses.filter {
      v =>
        v._fitness.isEmpty
    }
    if (unevaluatedHypotheses.nonEmpty) {
      val rdds: Seq[RDD[SubSeq]] = unevaluatedHypotheses.map {
        h =>
          h.indexRDD
      }
      val actionCosts = conf.actionCosts
      val costRDDs: Seq[RDD[Double]] = rdds.map {
        v =>
          v.map {
            subseq =>
              import subseq._
              val allTraces = broadcastedTraces.value
              val traces: Seq[Trace] = seqI.map {
                i =>
                  allTraces(i)
              }
              traces.map {
                tr =>
                  List(UseFleet(List(drone))) ++ tr
              }
          }
            .map {
              seq =>
                actionCosts.estimate(seq.iterator, spooky)
            }
      }
      val reduced: Seq[Double] = SpookyUtils.RDDs.batchReduce(costRDDs) {
        Math.max
      }
      unevaluatedHypotheses.zip(reduced).foreach {
        tuple =>
          tuple._1._fitness = Some(tuple._2)
      }
    }
  }

  //all solutions refers to the same matrix
  //travelling path of each drone.
  //outer index indicates group, inner index indicates index of the edges being traversed.
  //include start and end index, start index is hardcoded and cannot be changed.
  //the last 3 parameters must have identical cadinality
  //total distance can be easily calculated
  case class Hypothesis(
                         indexRDD: RDD[SubSeq]
                       ) extends Chromosome {
    allHypotheses += this

    var _fitness: Option[Double] = None

    /**
      * will batch evaluate fitnesses of all hypotheses that hasn't been evaluated using RDD zipping trick
      * theoretically, since fitness is only used on population rather than chomosome. only 1 evaluation is required for a batch
      * it should work with any Population implementations
      */
    override def fitness(): Double = {
      _fitness.getOrElse {
        evaluateMissingFitness()
        _fitness.get
      }
    }
  }

  case class Selection() extends TournamentSelection(4) {
  }

  case class Crossover() extends CrossoverPolicy {

    override def crossover(first: Chromosome, second: Chromosome): ChromosomePair = {
      (first, second) match {
        case (Hypothesis(rdd1), Hypothesis(rdd2)) =>

        case _ =>
          throw new MathIllegalArgumentException(LocalizedFormats.UNSUPPORTED_OPERATION, first, second)
      }
    }
  }

  case class Mutation() extends MutationPolicy {

    override def mutate(original: Chromosome): Chromosome = {
      original match {
        case Hypothesis(rdd) =>
          val rnd = Random.nextInt(4)
          rnd match {
            case 0 =>

            case 1 =>
            case 2 =>
            case 3 =>
          }
        case v@ _ =>
          throw new MathIllegalArgumentException(LocalizedFormats.UNSUPPORTED_OPERATION, v)
      }
    }
  }
}
