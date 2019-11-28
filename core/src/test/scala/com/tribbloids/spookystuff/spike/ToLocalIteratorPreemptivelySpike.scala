package com.tribbloids.spookystuff.spike

import java.util.concurrent.ArrayBlockingQueue

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{FutureAction, SparkContext}
import org.scalatest.{FunSpec, Ignore}

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

// TODO: delete after https://issues.apache.org/jira/browse/SPARK-29852 is accepted!
@Ignore
class ToLocalIteratorPreemptivelySpike extends FunSpec {

  import ToLocalIteratorPreemptivelySpike._

  lazy val sc: SparkContext = SparkSession.builder().master("local[*]").getOrCreate().sparkContext

  it("can be much faster than toLocalIterator") {

    val max = 80
    val delay = 100

    val slowRDD = sc.parallelize(1 to max, 8).map { v =>
      Thread.sleep(delay)
      v
    }

    val (r1, t1) = timed {
      slowRDD.toLocalIterator.toList
    }

    val capacity = 4
    val (r2, t2) = timed {
      slowRDD.toLocalIteratorPreemptively(capacity).toList
    }

    assert(r1 == r2)
    println(s"linear: $t1, preemptive: $t2")
    assert(t1 > t2 * 2)
    assert(t2 > max * delay / capacity)
  }
}

object ToLocalIteratorPreemptivelySpike {

  case class PartitionExecution[T: ClassTag](
      @transient self: RDD[T],
      id: Int
  ) {

    def eager: this.type = {
      AsArray.future
      this
    }

    case object AsArray {

      @transient lazy val future: FutureAction[Array[T]] = {
        var result: Array[T] = null

        val future = self.context.submitJob[T, Array[T], Array[T]](
          self,
          _.toArray,
          Seq(id), { (_, data) =>
            result = data
          },
          result
        )

        future
      }

      @transient lazy val now: Array[T] = future.get()
    }
  }

  implicit class RDDFunctions[T: ClassTag](self: RDD[T]) {

    import scala.concurrent.ExecutionContext.Implicits.global

    def _toLocalIteratorPreemptively(capacity: Int): Iterator[Array[T]] = {
      val executions = self.partitions.indices.map { ii =>
        PartitionExecution(self, ii)
      }

      val buffer = new ArrayBlockingQueue[Try[PartitionExecution[T]]](capacity)

      Future {
        executions.foreach { exe =>
          buffer.put(Success(exe)) // may be blocking due to capacity
          exe.eager // non-blocking
        }
      }.onFailure {
        case e: Throwable =>
          buffer.put(Failure(e))
      }

      self.partitions.indices.toIterator.map { _ =>
        val exe = buffer.take().get
        exe.AsArray.now
      }
    }

    def toLocalIteratorPreemptively(capacity: Int): Iterator[T] = {

      _toLocalIteratorPreemptively(capacity).flatten
    }
  }

  def timed[T](fn: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val result = fn
    val endTime = System.currentTimeMillis()
    (result, endTime - startTime)
  }
}
