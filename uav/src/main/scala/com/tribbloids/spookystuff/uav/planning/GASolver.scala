package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.commons.math3.exception.MathIllegalArgumentException
import org.apache.commons.math3.exception.util.LocalizedFormats
import org.apache.commons.math3.genetics._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class Route(
                  linkOpt: Option[Link],
                  is: Seq[Int]
                ) {

  def toTracesOpt(allTraces: Seq[Trace]): Option[Seq[Trace]] = {
    linkOpt.map {
      link =>
        val traces: Seq[Trace] = is.map {
          i =>
            allTraces(i)
        }
        val seq = traces.map {
          tr =>
            List(PreferLink(link)) ++ tr
        }
        seq
    }
  }

  def estimatePartialCost(solver: GASolver): Double = {

    val seqOpt = toTracesOpt(solver.allTracesBroadcasted.value)
    seqOpt.map {
      seq =>
        solver.actionCosts.estimate(seq, solver.spooky)
    }
      .getOrElse {
        Double.MaxValue
      }
  }

  def optimalInsertFrom(from: Seq[Int], solver: GASolver): Route = {
    var state = this
    for (i <- from) {
      // insert into left that yield the best cost
      val size = state.is.size
      val candidates_costs = (0 to size).map {
        j =>
          val splitted = state.is.splitAt(j)
          val inserted = splitted._1 ++ Seq(i) ++ splitted._2
          val insertedRoute = this.copy(is = inserted)
          val cost = insertedRoute.estimatePartialCost(solver)
          insertedRoute -> cost
      }
        .sortBy(_._2)
      //      LoggerFactory.getLogger(this.getClass).debug(
      //        MessageView(candidates_costs).toJSON()
      //      )
      state = candidates_costs.head._1
    }
    state
  }
}

/**
  * Multi-Depot k-Rural Postman Problem solver
  * genetic algorithm comes in handy.
  * use 1 shuffling per generation.
  * Unfortunately this may be suboptimal comparing to http://niels.nu/blog/2016/spark-of-life-genetic-algorithm.html
  * which has many micro local generations per shuffling.
  * takes further testing to know if the convenience of local estimation worth more shuffling.
  */
case class GASolver(
                     @transient private val allTraces: List[Trace],
                     spooky: SpookyContext
                   ) { //TODO: NOTSerializable?

  import com.tribbloids.spookystuff.utils.SpookyViews._

  val allTracesBroadcasted = spooky.sparkContext.broadcast(allTraces)
  val allIndicesRDD = spooky.sparkContext.parallelize(allTraces.indices).persist()

  @transient lazy val conf = spooky.submodule[UAVConf]
  def actionCosts = conf.actionCosts

  @transient val allHypotheses: ArrayBuffer[Hypothesis] = ArrayBuffer.empty

  def evaluatePendingFitness(): Unit = this.synchronized{
    val unevaluatedHypotheses = GASolver.this.allHypotheses.filter {
      v =>
        v._fitness.isEmpty
    }
    if (unevaluatedHypotheses.nonEmpty) {
      val rdds: Seq[RDD[Route]] = unevaluatedHypotheses.map {
        h =>
          h.indexRDD
      }
      val costRDDs: Seq[RDD[Double]] = rdds.map {
        v =>
          v.map {
            subseq =>
              subseq.estimatePartialCost(this)
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
                         indexRDD: RDD[Route]
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
        evaluatePendingFitness()
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
          val zipped = rdd1.zip(rdd2).zipWithIndex()
          val numRoutes = zipped.partitions.length
          val iRouteSelected = Random.nextInt(numRoutes)
          val routesSelected: (Route, Route) = zipped.filter {
            wi =>
              wi._2 == iRouteSelected
          }
            .first()._1

          val swapped = zipped.map {
            wi =>
              assert(wi._1._1.linkOpt == wi._1._2.linkOpt)
              val linkOpt = wi._1._1.linkOpt
              val leftCleaned = wi._1._1.is.filterNot {
                i =>
                  routesSelected._2.is.contains(i)
              }
              val rightCleaned = wi._1._2.is.filterNot {
                i =>
                  routesSelected._1.is.contains(i)
              }
              val left = Route(linkOpt, leftCleaned)
              val right = Route(linkOpt, rightCleaned)
              if (wi._2 == iRouteSelected) {
                val leftInserted = left.optimalInsertFrom(routesSelected._2.is, GASolver.this)
                val rightInserted = right.optimalInsertFrom(routesSelected._1.is, GASolver.this)
                leftInserted -> rightInserted
              }
              else {
                left -> right
              }
          }
          val rddLeft = swapped.keys
          val rddRight = swapped.values
          new ChromosomePair(
            Hypothesis(rddLeft),
            Hypothesis(rddRight)
          )
        case _ =>
          throw new MathIllegalArgumentException(LocalizedFormats.UNSUPPORTED_OPERATION, first, second)
      }
    }
  }

  def sampleWithoutReplacement(
                                n: Int = 1,
                                exclude: Seq[Int] = Nil,
                                max: Int = allTraces.size
                              ): Seq[Int] = {

    var next = -1
    while(next <= 0) {
      val h = Random.nextInt(max)
      if (!exclude.contains(h)) next = h
    }
    sampleWithoutReplacement(n - 1, exclude :+ next, max) :+ next
  }

  def swap(rdd: RDD[Route]): RDD[Route] = {
    val pair = this.sampleWithoutReplacement(2)
    rdd.map {
      subseq =>
        val mutated = subseq.is.map {
          i =>
            if (i == pair.head) pair.last
            else if (i == pair.last) pair.head
            else i
        }
        subseq.copy(is = mutated)
    }
  }

  def insert(rdd: RDD[Route]): RDD[Route] = {
    val pair = this.sampleWithoutReplacement(2)
    rdd.map {
      subseq =>
        val mutated = subseq.is.flatMap {
          i =>
            if (i == pair.head) Nil
            else if (i == pair.last) pair //TODO: not covering insert at the end of the queue.
            else Seq(i)
        }
        subseq.copy(is = mutated)
    }
  }

  case class Mutation() extends MutationPolicy {

    override def mutate(original: Chromosome): Chromosome = {
      original match {
        case Hypothesis(rdd) =>
          val rnd = Random.nextInt(2)
          rnd match {
            case 0 =>
              Hypothesis(swap(rdd))
            case 1 =>
              Hypothesis(insert(rdd))
          }
        case v@ _ =>
          throw new MathIllegalArgumentException(LocalizedFormats.UNSUPPORTED_OPERATION, v)
      }
    }
  }

  //  lazy val linkOptRDD: RDD[Option[Link]] = {
  //    val rdd = spooky.sparkContext.mapPerExecutorCore {
  //      val uavs = spooky.submodule[UAVConf].uavsRandomList
  //      val linkOpt = spooky.withSession {
  //        session =>
  //          Link.trySelect(
  //            uavs,
  //            session
  //          )
  //            .toOption
  //      }
  //      linkOpt
  //    }
  //    rdd.persist()
  //    rdd.count()
  //    rdd
  //  }

  //TODO: takes 1 stage, not efficient! sad!
  def generate1Seed(sc: SparkContext): RDD[Route] = {

    val shuffled = allIndicesRDD.shuffle
    val routeRDD = shuffled.mapPartitions {
      itr =>
        val uavs = spooky.submodule[UAVConf].uavsRandomList
        val linkOpt = spooky.withSession {
          session =>
            Link.trySelect(
              uavs,
              session
            )
              .toOption
        }
        Iterator(Route(linkOpt, itr.toSeq))
    }
    routeRDD
  }

  //  def run(): Unit = {
  //    val ga = new GeneticAlgorithm(
  //      Crossover(),
  //      1,
  //      Mutation(),
  //      0.10,
  //      Selection()
  //    )
  //  }
}
