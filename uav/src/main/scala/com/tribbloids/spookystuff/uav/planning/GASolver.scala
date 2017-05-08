package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.utils.{NOTSerializable, SpookyUtils}
import org.apache.commons.math3.exception.MathIllegalArgumentException
import org.apache.commons.math3.exception.util.LocalizedFormats
import org.apache.commons.math3.genetics._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Success, Try}

case class Route(
                  linkTry: Try[Link],
                  is: Seq[Int]
                ) extends NOTSerializable {

  def toTracesOpt(allTraces: Seq[Trace]): Option[Seq[Trace]] = {
    linkTry.toOption.map {
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

  def estimateCost(solver: GASolver): Double = {

    linkTry match {
      case Success(link) =>

    }
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
          val cost = insertedRoute.estimateCost(solver)
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
                     @transient allTraces: List[Trace],
                     spooky: SpookyContext
                   ) extends Serializable {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  val allTracesBroadcasted = spooky.sparkContext.broadcast(allTraces)

  def conf = spooky.submodule[UAVConf]
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
          h.rdd
      }
      val costRDDs: Seq[RDD[Double]] = rdds.map {
        v =>
          v.map {
            subseq =>
              subseq.estimateCost(this)
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
                         rdd: RDD[Route]
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

    if (n > 1) sampleWithoutReplacement(n - 1, exclude :+ next, max) :+ next
    else Seq(next)
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
      h =>
        val mutated = h.is.flatMap {
          i =>
            if (i == pair.head) Nil
            else if (i == pair.last) pair //TODO: not covering insert at the end of the queue.
            else Seq(i)
        }
        h.copy(is = mutated)
    }
  }

  case object Selection extends TournamentSelection(4) {
  }

  case object Mutation extends MutationPolicy {

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

  case object Crossover extends CrossoverPolicy {

    override def crossover(first: Chromosome, second: Chromosome): ChromosomePair = {
      (first, second) match {
        case (Hypothesis(rdd1), Hypothesis(rdd2)) =>
          val zipped = rdd1.zipPartitions(rdd2){
            (i1, i2) =>
              val r1 = i1.next()
              val r2 = i2.next()
              val seq2 = i2.toSeq
              assert(r1.linkTry == r2.linkTry)
              assert(i1.isEmpty)
              assert(i2.isEmpty)
              Iterator(r1 -> r2)
          }
          val wIndex = zipped.zipWithIndex()
          val numRoutes = wIndex.partitions.length
          val iRouteSelected = Random.nextInt(numRoutes)
          val routesSelected: (Seq[Int], Seq[Int]) = wIndex.filter {
            wi =>
              wi._2 == iRouteSelected
          }
            .map {
              _._1 match {
                case (r1, r2) =>
                  r1.is -> r2.is
              }
            }
            .first()

          val swapped = wIndex.map {
            wi =>
              assert(wi._1._1.linkTry == wi._1._2.linkTry)
              val linkOpt = wi._1._1.linkTry
              val leftCleaned = wi._1._1.is.filterNot {
                i =>
                  routesSelected._2.contains(i)
              }
              val rightCleaned = wi._1._2.is.filterNot {
                i =>
                  routesSelected._1.contains(i)
              }
              val left = Route(linkOpt, leftCleaned)
              val right = Route(linkOpt, rightCleaned)
              if (wi._2 == iRouteSelected) {
                val leftInserted = left.optimalInsertFrom(routesSelected._2, GASolver.this)
                val rightInserted = right.optimalInsertFrom(routesSelected._1, GASolver.this)
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

  def getLinkRDD: RDD[Link] = {
    import com.tribbloids.spookystuff.uav.utils.UAVViews._
    val proto: RDD[Link] = spooky.sparkContext.mapPerExecutorCore {
      val linkTry = spooky.withSession {
        session =>
          session.linkTry
      }
      linkTry
    }
      .flatMap(_.toOption)
    proto
  }

  def generateSeedPairs(
                         numTraces: Int,
                         numSeeds: Int
                       ): RDD[(Link, Seq[Seq[Int]])] = {
    val seeds = (1 to numSeeds).map {
      i =>
        Random.nextLong()
    }
    val proto: RDD[Link] = getLinkRDD
    proto.persist()
    val numLinks = proto.count().toInt

    val pairs = proto
      .zipWithIndex()
      .map {
        tuple =>
          val is: Seq[Seq[Int]] = seeds.map {
            seed =>
              val random = new Random(seed)
              val inPartition = (0 until numTraces).flatMap {
                i =>
                  val partition = random.nextInt(numLinks) % numLinks
                  if (partition == tuple._2) Some(i)
                  else None
              }
              val result = Random.shuffle(inPartition)
              result
          }
          tuple._1 -> is
      }

    pairs.persist()
    pairs.count()

    proto.unpersist()

    pairs
  }

  def generateSeeds(
                     numSeeds: Int
                   ): Seq[Hypothesis] = {
    val pairs = generateSeedPairs(this.allTraces.size, numSeeds)
    (0 until numSeeds).map {
      i =>
        val subRDD = pairs.map {
          case (link, iss) =>
            Route(Success(link), iss(i))
        }
        Hypothesis(subRDD)
    }
  }
}
